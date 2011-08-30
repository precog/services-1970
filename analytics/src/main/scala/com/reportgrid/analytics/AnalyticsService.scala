package com.reportgrid.analytics

import blueeyes._
import blueeyes.concurrent.Future
import blueeyes.core.data.{Chunk, ByteChunk, BijectionsChunkJson, BijectionsChunkString}
import blueeyes.core.http._
import blueeyes.core.http.MimeTypes.{application, json}
import blueeyes.core.service._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.{JPath, JsonParser, JPathField}
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.JodaSerializationImplicits._
import blueeyes.persistence.mongo._
import blueeyes.persistence.cache.{Stage, ExpirationPolicy, CacheSettings}
import blueeyes.util.{Clock, ClockSystem, PartialFunctionCombinators}
import HttpStatusCodes.{BadRequest, Unauthorized, Forbidden}

import net.lag.configgy.{Configgy, ConfigMap}

import org.joda.time.Instant
import org.joda.time.DateTime

import java.net.URL
import java.util.concurrent.TimeUnit

import com.reportgrid.analytics.AggregatorImplicits._
import com.reportgrid.blueeyes.ReportGridInstrumentation
import com.reportgrid.api.Server
import com.reportgrid.api.ReportGridTrackingClient
import com.reportgrid.api.blueeyes._
import scala.collection.immutable.SortedMap
import scala.collection.immutable.IndexedSeq
import scalaz.Scalaz._

case class YggdrasilConfig(host: String, port: Option[Int], path: String)

case class AnalyticsState(aggregationEngine: AggregationEngine, tokenManager: TokenManager, clock: Clock, auditClient: ReportGridTrackingClient[JValue], yggdrasil: YggdrasilConfig)

trait AnalyticsService extends BlueEyesServiceBuilder with BijectionsChunkJson with BijectionsChunkString with ReportGridInstrumentation {
  import AggregationEngine._
  import AnalyticsService._
  import AnalyticsServiceSerialization._

  def mongoFactory(configMap: ConfigMap): Mongo

  def auditClientFactory(configMap: ConfigMap): ReportGridTrackingClient[JValue] 

  //val yggdrasilClient: HttpClient[JValue] = (new HttpClientXLightWeb).translate[JValue]

  val analyticsService = service("analytics", "1.0") {
    logging { logger =>
      healthMonitor { monitor => context =>
        startup {
          import context._

          val mongoConfig = config.configMap("mongo")

          val mongo = mongoFactory(mongoConfig)

          val database = mongo.database(mongoConfig.getString("database").getOrElse("analytics"))

          val tokensCollection = mongoConfig.getString("tokensCollection").getOrElse("tokens")

          val auditClient = auditClientFactory(config.configMap("audit"))

          val yggConfigMap = config.configMap("yggdrasil")
          val yggConfig = YggdrasilConfig(
            yggConfigMap.getString("host", "api.reportgrid.com"),
            yggConfigMap.getInt("port"),
            yggConfigMap.getString("path", "/services/yggdrasil/v0")
          )

          for {
            tokenManager      <- TokenManager(database, tokensCollection)
            aggregationEngine <- AggregationEngine(config, logger, database)
          } yield {
            AnalyticsState(aggregationEngine, tokenManager, ClockSystem.realtimeClock, auditClient, yggConfig)
          }
        } ->
        request { (state: AnalyticsState) =>
          import state._

          def renderHistogram(histogram: Traversable[(JValue, Long)]): JObject = {
            histogram.foldLeft(JObject.empty) {
              case (content, (value, count)) =>
                val name = JPathField(renderNormalized(value))

                content.set(name, JInt(count)) --> classOf[JObject]
            }
          }

          def tokenOf(request: HttpRequest[_]): Future[Token] = {
            request.parameters.get('tokenId) match {
              case None =>
                throw HttpException(BadRequest, "A tokenId query parameter is required to access this URL")

              case Some(tokenId) =>
                tokenManager.lookup(tokenId).map { token =>
                  token match {
                    case None =>
                      throw HttpException(BadRequest, "The specified token does not exist")

                    case Some(token) =>
                      if (token.expired) throw HttpException(Unauthorized, "The specified token has expired")

                      token
                  }
                }
            }
          }

          def withTokenAndPath[T](request: HttpRequest[_])(f: (Token, Path) => Future[T]): Future[T] = {
            tokenOf(request) flatMap { token => f(token, fullPathOf(token, request)) }
          }

          val audit = auditor[JValue, JValue](auditClient, clock, tokenOf)
          
          def yggdrasilRewrite[T](req: HttpRequest[T]): Option[HttpRequest[T]] = {
            import HttpHeaders._
            (!req.headers.header[`User-Agent`].exists(_.value == ReportGridUserAgent)).option {
              val prefixPath = req.parameters.get('prefixPath).getOrElse("")
              req.copy(
                uri = req.uri.copy(
                  host = Some(yggdrasil.host), 
                  port = yggdrasil.port, 
                  path = Some(yggdrasil.path + "/vfs/" + prefixPath)
                ),
                parameters = req.parameters - 'prefixPath
              ) 
            }
          }

          def geoipLocationHierarchy(request: HttpRequest[_]): Hierarchy = {
            error("todo")
          }

          def getTagSet(result: Tag.ExtractionResult) = result match {
            case Tag.Skipped => Set.empty[Tag]
            case Tag.Tags(tags) => tags.toSet
            case Tag.Errors(errors) =>
              val errmsg = "Errors occurred extracting tag information: " + errors.map(_.toString).mkString("; ")
              throw new HttpException(BadRequest, errmsg)
          }

          jsonp {
            /* The virtual file system, which is used for storing data,
             * retrieving data, and querying for metadata.
             */
            path("""/vfs/(?:(?<prefixPath>(?:[^\n.](?:[^\n/]|/[^\n\.])+)/?)?)""") { 
              $ {
                /* Post data to the virtual file system.
                 */
                audit("track") {
                  //forwarding(yggdrasilRewrite[JValue])(yggdrasilClient) {
                    post { request: HttpRequest[JValue] =>
                      withTokenAndPath(request) { (token, path) => 
                        val tagExtractors = Tag.timeTagExtractor(timeSeriesEncoding, TimeReference(timeSeriesEncoding, clock.instant())) ::
                                            Tag.locationTagExtractor(geoipLocationHierarchy(request)) ::
                                            Nil

                        request.content.foreach { 
                          case obj @ JObject(fields) => 
                            val count: Int = request.parameters.get('count).map(_.toInt)
                                             .orElse((obj \? "count").flatMap(_.validated[Int].toOption))
                                             .getOrElse(1)

                            val (tagResults, _) = Tag.extractTags(tagExtractors, obj)
                            val tags = getTagSet(tagResults)

                            (obj \ "events") match {
                              case JObject(fields) => 
                                fields.foreach {
                                  case JField(eventName, event: JObject) => 
                                    aggregationEngine.aggregate(token, path, eventName, tags, event, count)

                                  case _ => 
                                    throw new HttpException(BadRequest, "Events must be JSON objects.")
                                }

                              case err => 
                                throw new HttpException(BadRequest, "Unexpected type for field \"events\".")
                            }
                        }

                        Future.sync(HttpResponse[JValue](content = None))
                      }
                    }
                  //}
                } ~
                audit("explore paths") {
                  get { request: HttpRequest[JValue] =>
                    withTokenAndPath(request) { (token, path) => 
                      aggregationEngine.getPathChildren(token, path).map(_.serialize.ok)
                    }
                  }
                }
              } ~
              path("""(?<variable>\.[^\n/]+)""") {
                $ {
                  audit("explore variables") {
                    get { request: HttpRequest[JValue] =>
                      val variable = variableOf(request)

                      withTokenAndPath(request) { (token, path) => 
                        aggregationEngine.getVariableChildren(token, path, variable).map(_.map(_.child).serialize.ok)
                      }
                    }
                  }
                } ~
                path("/") {
                  path("statistics") {
                    audit("variable statistics") {
                      get { request: HttpRequest[JValue] =>
                        val variable = variableOf(request)

                        withTokenAndPath(request) { (token, path) => 
                          aggregationEngine.getVariableStatistics(token, path, variable).map(_.serialize.ok)
                        }
                      }
                    }
                  } ~
                  path("count") {
                    audit("variable occurrence count") {
                      post { request: HttpRequest[JValue] =>
                        val variable = variableOf(request)

                        withTokenAndPath(request) { (token, path) => 
                          aggregationEngine.getVariableCount(token, path, variable, tagTerms(request.content, None)).map(_.serialize.ok)
                        }
                      }
                    }
                  } ~
                  path("series/") {
                    audit("variable occurrence series") {
                      path('periodicity) {
                        $ {
                          queryVariableSeries(tokenOf, _.count, aggregationEngine)
                        } ~ 
                        path("/") {
                          path("means") {
                            queryVariableSeries(tokenOf, _.mean, aggregationEngine)
                          } ~ 
                          path("standardDeviations") {
                            queryVariableSeries(tokenOf, _.standardDeviation, aggregationEngine) 
                          } 
                        }
                      }
                    }
                  } ~
                  path("histogram/") {
                    $ {
                      audit("variable histogram") {
                        get { request: HttpRequest[JValue] =>
                          val variable = variableOf(request)

                          withTokenAndPath(request) { (token, path) => 
                            aggregationEngine.getHistogram(token, path, variable).map(renderHistogram).map(_.ok)
                          }
                        }
                      }
                    } ~
                    path("top/'limit") {
                      audit("variable histogram top") {
                        get { request: HttpRequest[JValue] =>
                          val variable = variableOf(request)
                          val limit    = request.parameters('limit).toInt

                          withTokenAndPath(request) { (token, path) => 
                            aggregationEngine.getHistogramTop(token, path, variable, limit).map(renderHistogram).map(_.ok)
                          }
                        }
                      }
                    } ~
                    path("bottom/'limit") {
                      audit("variable histogram bottom") {
                        get { request: HttpRequest[JValue] =>
                          val variable = variableOf(request)
                          val limit    = request.parameters('limit).toInt
                          
                          withTokenAndPath(request) { (token, path) => 
                            aggregationEngine.getHistogramBottom(token, path, variable, limit).map(renderHistogram).map(_.ok)
                          }
                        }
                      }
                    }
                  } ~
                  path("length") {
                    audit("count of variable values") {
                      get { request: HttpRequest[JValue] =>
                        tokenOf(request).flatMap { token =>
                          val path     = fullPathOf(token, request)
                          val variable = variableOf(request)

                          aggregationEngine.getVariableLength(token, path, variable).map(_.serialize.ok)
                        }
                      }
                    }
                  } ~
                  path("values/") {
                    $ {
                      audit("list of variable values") {
                        get { request: HttpRequest[JValue] =>
                          tokenOf(request).flatMap { token =>
                            val path     = fullPathOf(token, request)
                            val variable = variableOf(request)

                            aggregationEngine.getValues(token, path, variable).map(_.toList.serialize.ok)
                          }
                        }
                      }
                    } ~
                    path("top/'limit") {
                      audit("list of top variable values") {
                        get { request: HttpRequest[JValue] =>
                          tokenOf(request).flatMap { token =>
                            val path     = fullPathOf(token, request)
                            val variable = variableOf(request)
                            val limit    = request.parameters('limit).toInt

                            aggregationEngine.getValuesTop(token, path, variable, limit).map(_.serialize.ok)
                          }
                        }
                      }
                    } ~
                    path("bottom/'limit") {
                      audit("list of bottom variable values") {
                        get { request: HttpRequest[JValue] =>
                          val variable = variableOf(request)
                          val limit    = request.parameters('limit).toInt

                          withTokenAndPath(request) { (token, path) => 
                            aggregationEngine.getValuesBottom(token, path, variable, limit).map(_.serialize.ok)
                          }
                        }
                      }
                    } ~
                    path('value) {
                      $ {
                        audit("explore a variable value") {
                          get { request: HttpRequest[JValue] =>
                            // return a list of valid subpaths
                            Future.sync(JArray(JString("count") :: JString("series/") :: Nil).ok[JValue])
                          }
                        }
                      } ~
                      path("/") {
                        path("count") {
                          audit("count occurrences of a variable value") {
                            post { request: HttpRequest[JValue] =>
                              val observation = JointObservation(HasValue(variableOf(request), valueOf(request)))

                              withTokenAndPath(request) { (token, path) => 
                                aggregationEngine.observationCount(token, path, observation, tagTerms(request.content, None)) map (_.serialize.ok)
                              }
                            }
                          }
                        } ~
                        path("series/") {
                          audit("variable value series") {
                            get { request: HttpRequest[JValue] =>
                              // simply return the names of valid periodicities that can be used for series queries
                              Future.sync(JArray(Periodicity.Default.map(p => JString(p.name))).ok[JValue])
                            } ~
                            path('periodicity) {
                              post { request: HttpRequest[JValue] =>
                                val periodicity = periodicityOf(request)
                                val observation = JointObservation(HasValue(variableOf(request), valueOf(request)))

                                withTokenAndPath(request) { (token, path) => 
                                  aggregationEngine.observationSeries(token, path, observation, tagTerms(request.content, Some(periodicity))) map (_.serialize.ok)
                                  //todo: make this work again
                                  // .map(groupTimeSeries(seriesGrouping(request)))
                                  // .map(_.fold(_.serialize, _.serialize).ok)
                                }
                              } 
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            } ~
            path("/search") {
              audit("count or series query") {
                post { request: HttpRequest[JValue] =>
                  tokenOf(request).flatMap { token =>
                    val content = request.content.getOrElse {
                      throw new HttpException(BadRequest, """request body was empty. "select", "from", and "properties" fields must be specified.""")
                    }
                      
                    val select = (content \ "select").deserialize[String].toLowerCase
                    val from = token.path / ((content \ "from").deserialize[String])
                    val observation = JointObservation((content \ "properties").deserialize[Set[HasValue]])

                    Selection(select) match {
                      case Count => 
                        aggregationEngine.observationCount(token, from, observation, tagTerms(Some(content), None)) map (_.serialize.ok)

                      case Series(periodicity) => 
                        aggregationEngine.observationSeries(token, from, observation, tagTerms(Some(content), Some(periodicity))) map (_.serialize.ok)
                        // todo: make this work again
                        //.map(groupTimeSeries(grouping))
                        //.map(_.fold(_.serialize, _.serialize).ok)

                      case Related => 
                        val finiteSpan = timeSpan(content).getOrElse {
                          throw new HttpException(BadRequest, "Start and end dates must be specified to query for values related to an observation.")
                        }

                        aggregationEngine.findRelatedInfiniteValues(token, from, observation, finiteSpan) map (_.serialize.ok)
                    }
                  }
                }
              }
            } ~
            path("/intersect") {
              audit("intersection query") {
                post { request: HttpRequest[JValue] => 
                  tokenOf(request).flatMap { token => 
                    import VariableDescriptor._
                    val content = request.content.getOrElse {
                      throw new HttpException(BadRequest, """request body was empty. "select", "from", and "properties" fields must be specified.""")
                    }
                      
                    val select = Selection((content \ "select").deserialize[String].toLowerCase)
                    val from: Path = token.path + "/" + (content \ "from").deserialize[String]
                    val properties = (content \ "properties").deserialize[List[VariableDescriptor]]

                    select match {
                      case Count => 
                        aggregationEngine.intersectCount(token, from, properties, tagTerms(Some(content), None))
                        .map(serializeIntersectionResult[CountType]).map(_.ok)

                      case Series(periodicity) =>
                        aggregationEngine.intersectSeries(token, from, properties, tagTerms(Some(content), Some(periodicity)))
                        //.map(_.map((groupTimeSeries(grouping)(_: TimeSeriesType).fold(_.serialize, _.serialize)).second)(collection.breakOut))
                        .map(serializeIntersectionResult[ResultSet[JObject, CountType]]).map(_.ok)
                    }
                  }
                } 
              }
            } ~ 
            path("/tokens/") {
              audit("token") {
                get { request: HttpRequest[JValue] =>
                  tokenOf(request) flatMap { token =>
                    tokenManager.listDescendants(token) map { descendants =>
                      descendants.map { descendantToken =>
                        descendantToken.tokenId.serialize
                      }
                    } map { 
                      JArray(_).ok[JValue]
                    }
                  }
                } ~
                post { request: HttpRequest[JValue] =>
                  tokenOf(request).flatMap { token =>
                    val content: JValue = request.content.getOrElse {
                      throw new HttpException(BadRequest, "New token must be contained in POST content")
                    }

                    val path        = (content \ "path").deserialize[Option[String]].getOrElse("/")
                    val permissions = (content \ "permissions").deserialize[Option[Permissions]].getOrElse(token.permissions)
                    val expires     = (content \ "expires").deserialize[Option[DateTime]].getOrElse(token.expires)
                    val limits      = (content \ "limits").deserialize[Option[Limits]].getOrElse(token.limits)

                    tokenManager.issueNew(token, path, permissions, expires, limits).map { newToken =>
                      HttpResponse[JValue](content = Some(newToken.tokenId.serialize))
                    }
                  }
                } ~
                path('descendantTokenId) {
                  get { request: HttpRequest[JValue] =>
                    tokenOf(request).flatMap { token =>
                      if (token.tokenId == request.parameters('descendantTokenId)) {
                        token.parentTokenId.map { parTokenId =>
                          tokenManager.lookup(parTokenId).map { parent => 
                            val sanitized = parent.map(token.relativeTo).map(_.copy(parentTokenId = None, accountTokenId = ""))
                            HttpResponse[JValue](content = sanitized.map(_.serialize))
                          }
                        } getOrElse {
                          Future.sync(HttpResponse[JValue](status = Forbidden))
                        }
                      } else {
                        tokenManager.getDescendant(token, request.parameters('descendantTokenId)).map { 
                          _.map { _.relativeTo(token).copy(accountTokenId = "").serialize }
                        } map { descendantToken =>
                          HttpResponse[JValue](content = descendantToken)
                        }
                      }
                    }
                  } ~
                  delete { (request: HttpRequest[JValue]) =>
                    tokenOf(request).flatMap { token =>
                      tokenManager.deleteDescendant(token, request.parameters('descendantTokenId)).map { _ =>
                        HttpResponse[JValue](content = None)
                      }
                    }
                  }
                }
              }
            }
          } 
        } ->
        shutdown { state =>
          state.aggregationEngine.stop
        }
      }
    }
  }
}

object AnalyticsService extends HttpRequestHandlerCombinators with PartialFunctionCombinators {
  import AnalyticsServiceSerialization._
  import AggregationEngine._

  def groupTimeSeries[R, T](periodicity: Option[Periodicity]) = (timeSeries: TimeSeries[T]) => {
    periodicity flatMap (timeSeries.groupBy) toRight timeSeries
  }

  def fullPathOf(token: Token, request: HttpRequest[_]): Path = {
    val prefixPath = request.parameters.get('prefixPath) match {
      case None | Some(null) => ""
      case Some(s) => s
    }

    token.path + "/" + prefixPath
  }

  def variableOf(request: HttpRequest[_]): Variable = Variable(JPath(request.parameters.get('variable).getOrElse("")))

  def valueOf(request: HttpRequest[_]): JValue = {
    import java.net.URLDecoder

    val value = request.parameters('value) // URLDecoder.decode(request.parameters('value), "UTF-8")

    try JsonParser.parse(value)
    catch {
      case _ => JString(value)
    }
  }

  def periodicityOf(request: HttpRequest[_]): Periodicity = {
    try Periodicity(request.parameters('periodicity))
    catch {
      case _ => throw HttpException(BadRequest, "Unknown or unspecified periodicity")
    }
  }

  def seriesGrouping(request: HttpRequest[_]): Option[Periodicity] = {
    request.parameters.get('groupBy).flatMap(Periodicity.byName)
  }

  def grouping(content: JValue) = (content \ "groupBy") match {
    case JNothing | JNull => None
    case jvalue   => Periodicity.byName(jvalue)
  }

  def timeSpan(content: JValue): Option[TimeSpan.Finite] = {
    val start = (content \ "start") match {
      case JNothing | JNull => None
      case jvalue   => Some(jvalue.deserialize[Instant])
    }

    val end = (content \ "end") match {
      case JNothing | JNull => None
      case jvalue   => Some(jvalue.deserialize[Instant])
    }

    (start <**> end)(TimeSpan(_, _))
  }

  def timeSpanTerm(content: JValue, p: Option[Periodicity]): Option[TagTerm] = {
    val periodicity = (content \ "periodicity") match {
      case JNothing | JNull | JBool(true) => p.orElse(Some(Periodicity.Eternity))
      //only if it is explicitly stated that no timestamp was used on submission do we exclude a time term
      case JBool(false) => None 
      case jvalue => p.orElse(Some(jvalue.deserialize[Periodicity]))
    }

    periodicity flatMap {
      case Periodicity.Eternity => 
        timeSpan(content).map(SpanTerm(timeSeriesEncoding, _)).orElse(Some(SpanTerm(timeSeriesEncoding, TimeSpan.Eternity)))

      case other => 
        timeSpan(content).map(IntervalTerm(timeSeriesEncoding, other, _)).orElse {
          throw new HttpException(BadRequest, "A periodicity was specified, but no finite time span could be determined.")
        }
    }
  }

  def locationTerm(content: JValue): Option[TagTerm] = (content \ "location") match {
    case JNothing | JNull => None
    case jvalue => Some(HierarchyLocationTerm("location", jvalue.deserialize[Hierarchy.Location]))
  }

  def tagTerms(requestContent: Option[JValue], p: Option[Periodicity]): List[TagTerm] = {
    requestContent.toList.flatMap { content => 
      List(timeSpanTerm(content, p), locationTerm(content)).flatten 
    } 
  }

  def queryVariableSeries[T: Decomposer : AbelianGroup](tokenOf: HttpRequest[_] => Future[Token], f: ValueStats => T, aggregationEngine: AggregationEngine) = {
    post { request: HttpRequest[JValue] =>
      tokenOf(request).flatMap { token =>
        val path        = fullPathOf(token, request)
        val variable    = variableOf(request)
        val periodicity = periodicityOf(request)

        aggregationEngine.getVariableSeries(token, path, variable, tagTerms(request.content, Some(periodicity))) map (_.mapValues(f).serialize.ok)
        // todo: make work again
        //.map(groupTimeSeries(seriesGrouping(request)))
        //.map(_.fold(_.map(f).serialize, _.map(f).serialize).ok)
      }
    } 
  }
}

object AnalyticsServiceSerialization extends AnalyticsSerialization {
  import AggregationEngine._

  // Decomposer is invariant, which means that this can't be reasonably implemented
  // as a Decomposer and used both for intersection results and grouped intersection
  // results.
  def serializeIntersectionResult[T: Decomposer](result: Iterable[(List[JValue], T)]) = {
    result.foldLeft[JValue](JObject(Nil)) {
      case (result, (values, x)) => 
        result.set(JPath(values.map(v => JPathField(renderNormalized(v)))), x.serialize)
    }
  }

  implicit def SortedMapDecomposer[K: Decomposer, V: Decomposer]: Decomposer[SortedMap[K, V]] = new Decomposer[SortedMap[K, V]] {
    override def decompose(m: SortedMap[K, V]): JValue = JArray(
      m.map(t => JArray(List(t._1.serialize, t._2.serialize))).toList
    )
  }

  implicit def TimeSeriesDecomposer[T: Decomposer]: Decomposer[TimeSeries[T]] = new Decomposer[TimeSeries[T]] {
    override def decompose(t: TimeSeries[T]) = JObject(
      JField("type", JString("timeseries")) ::
      JField("periodicity", t.periodicity.serialize) ::
      JField("data", t.series.serialize) ::
      Nil
    )
  }

  implicit def DeltaSetDecomposer[A: Decomposer, D: Decomposer, V : Decomposer : AbelianGroup]: Decomposer[DeltaSet[A, D, V]] = new Decomposer[DeltaSet[A, D, V]] {
    def decompose(value: DeltaSet[A, D, V]): JValue = JObject(
      JField("type", JString("deltas")) ::
      JField("zero", value.zero.serialize) ::
      JField("data", value.data.serialize) ::
      Nil
    )
  }

  implicit val StatisticsDecomposer: Decomposer[Statistics] = new Decomposer[Statistics] {
    def decompose(v: Statistics): JValue = JObject(
      JField("n",  v.n.serialize) ::
      JField("min",  v.min.serialize) ::
      JField("max",  v.max.serialize) ::
      JField("mean", v.mean.serialize) ::
      JField("variance", v.variance.serialize) ::
      JField("standardDeviation", v.standardDeviation.serialize) ::
      Nil
    )
  }

  implicit val VariableValueDecomposer: Decomposer[(Variable, HasValue)] = new Decomposer[(Variable, HasValue)] {
    def decompose(v: (Variable, HasValue)): JValue = JObject(
      JField("variable", v._1.serialize) :: 
      JField("value", v._2.serialize) ::
      Nil
    )
  }

//  implicit val HasValueExtractor: Extractor[HasValue] = new Extractor[HasValue] {
//    def extract(v: JValue): HasValue = {
//      (v \ "where").children.collect {
//        case JField(name, value) => HasValue(Variable(JPath(name)), value)
//      }.toSet
//    }
//  }
}
