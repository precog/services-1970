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

  val yggdrasilClient: HttpClient[JValue] = (new HttpClientXLightWeb).translate[JValue]

  val analyticsService = service("analytics", "0.02") {
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

          def renderHistogram(histogram: Traversable[(HasValue, Long)]): JObject = {
            histogram.foldLeft(JObject.empty) {
              case (content, (hasValue, count)) =>
                val name = JPathField(renderNormalized(hasValue.value))

                content.set(name, JInt(count)) --> classOf[JObject]
            }
          }

          def tokenOf(request: HttpRequest[_]): Future[Token] = {
            request.parameters.get('tokenId) match {
              case None =>
                throw HttpException(HttpStatusCodes.BadRequest, "A tokenId query parameter is required to access this URL")

              case Some(tokenId) =>
                tokenManager.lookup(tokenId).map { token =>
                  token match {
                    case None =>
                      throw HttpException(HttpStatusCodes.BadRequest, "The specified token does not exist")

                    case Some(token) =>
                      if (token.expired) throw HttpException(HttpStatusCodes.Unauthorized, "The specified token has expired")

                      token
                  }
                }
            }
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

          jsonp {
            /* The virtual file system, which is used for storing data,
             * retrieving data, and querying for metadata.
             */
            path("""/vfs/(?:(?<prefixPath>(?:[^\n.](?:[^\n/]|/[^\n\.])+)/?)?)""") { 
              $ {
                /* Post data to the virtual file system.
                 */
                audit("track") {
                  forwarding(yggdrasilRewrite[JValue])(yggdrasilClient) {
                    post { request: HttpRequest[JValue] =>
                      tokenOf(request).map { token =>
                        val path = fullPathOf(token, request)

                        request.content.foreach { content =>
                          val timestamp: Instant = (content \ "timestamp") match {
                            case JNothing => clock.instant()
                            case jvalue   => jvalue.deserialize[Instant]
                          }

                          val events: JObject = (content \ "events") match {
                            case jobject: JObject => jobject
                            case _ => sys.error("Expecting to find .events field containing object to aggregate")
                          }

                          val count: Int = (content \ "count") match {
                            case JNothing => 1
                            case jvalue   => jvalue.deserialize[Int]
                          }

                          aggregationEngine.aggregate(token, path, timestamp, events, count)
                        }

                        HttpResponse[JValue](content = None)
                      }
                    }
                  }
                } ~
                audit("explore paths") {
                  get { request: HttpRequest[JValue] =>
                    tokenOf(request).flatMap { token =>
                      val path = fullPathOf(token, request)

                      aggregationEngine.getPathChildren(token, path).map(_.serialize.ok)
                    }
                  }
                }
              } ~
              path("""(?<variable>\.[^\n/]+)""") {
                $ {
                  audit("explore variables") {
                    get { request: HttpRequest[JValue] =>
                      tokenOf(request).flatMap { token =>
                        val path     = fullPathOf(token, request)
                        val variable = variableOf(request)

                        aggregationEngine.getVariableChildren(token, path, variable).map(_.serialize.ok)
                      }
                    }
                  }
                } ~
                path("/") {
                  path("statistics") {
                    audit("variable statistics") {
                      get { request: HttpRequest[JValue] =>
                        tokenOf(request).flatMap { token =>
                          val path     = fullPathOf(token, request)
                          val variable = variableOf(request)

                          aggregationEngine.getVariableStatistics(token, path, variable).map(_.serialize.ok)
                        }
                      }
                    }
                  } ~
                  path("count") {
                    audit("variable occurrence count") {
                      get { request: HttpRequest[JValue] =>
                        tokenOf(request).flatMap { token =>
                          val path     = fullPathOf(token, request)
                          val variable = variableOf(request)

                          aggregationEngine.getVariableCount(token, path, variable).map(_.serialize.ok)
                        }
                      }
                    }
                  } ~
                  path("series/") {
                    audit("variable series") {
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
                          tokenOf(request).flatMap { token =>
                            val path     = fullPathOf(token, request)
                            val variable = variableOf(request)

                            aggregationEngine.getHistogram(token, path, variable).map(renderHistogram).map(_.ok)
                          }
                        }
                      }
                    } ~
                    path("top/'limit") {
                      audit("variable histogram top") {
                        get { request: HttpRequest[JValue] =>
                          tokenOf(request).flatMap { token =>
                            val path     = fullPathOf(token, request)
                            val variable = variableOf(request)
                            val limit    = request.parameters('limit).toInt

                            aggregationEngine.getHistogramTop(token, path, variable, limit).map(renderHistogram).map(_.ok)
                          }
                        }
                      }
                    } ~
                    path("bottom/'limit") {
                      audit("variable histogram bottom") {
                        get { request: HttpRequest[JValue] =>
                          tokenOf(request).flatMap { token =>
                            val path     = fullPathOf(token, request)
                            val variable = variableOf(request)
                            val limit    = request.parameters('limit).toInt

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
                          tokenOf(request).flatMap { token =>
                            val path     = fullPathOf(token, request)
                            val variable = variableOf(request)
                            val limit    = request.parameters('limit).toInt

                            aggregationEngine.getValuesBottom(token, path, variable, limit).map(_.serialize.ok)
                          }
                        }
                      }
                    } ~
                    path('value) {
                      $ {
                        audit("explore a variable value") {
                          get { request: HttpRequest[JValue] =>
                            tokenOf(request).map { token =>
                              val path     = fullPathOf(token, request)
                              val variable = variableOf(request)
                              val value    = valueOf(request)

                              JArray(
                                JString("series/") ::
                                JString("geo/") ::
                                JString("count") ::
                                Nil
                              ).ok[JValue]
                            }
                          }
                        }
                      } ~
                      path("/") {
                        path("count") {
                          audit("count occurrences of a variable value") {
                            get { request: HttpRequest[JValue] =>
                              tokenOf(request).flatMap { token =>
                                val path     = fullPathOf(token, request)
                                val observation = Obs.ofValue(variableOf(request), valueOf(request))
                                aggregationEngine.searchCount(token, path, observation).map(_.serialize.ok)
                              }
                            }
                          }
                        } ~
                        path("series/") {
                          audit("variable value series") {
                            get { request: HttpRequest[JValue] =>
                              tokenOf(request).map { token =>
                                val path     = fullPathOf(token, request)
                                val variable = variableOf(request)
                                val value    = valueOf(request)

                                JArray(Periodicity.Default.map(p => JString(p.name))).ok[JValue]
                              }
                            } ~
                            path('periodicity) {
                              getRange { (ranges, unit) => (request: HttpRequest[JValue]) =>
                                tokenOf(request).flatMap { token =>
                                  val path        = fullPathOf(token, request)
                                  val periodicity = periodicityOf(request)
                                  val observation = Obs.ofValue(variableOf(request), valueOf(request))

                                  unit.toLowerCase match {
                                    case "time" =>
                                      val (start, end) = ranges.head

                                      aggregationEngine.searchSeries(token, path, observation, periodicity, Some(new Instant(start)), Some(new Instant(end)))
                                      .map(groupTimeSeries(seriesGrouping(request)))
                                      .map(_.fold(_.serialize, _.serialize).ok)

                                    case _ => throw HttpException(HttpStatusCodes.BadRequest, "GET with Range only accepts unit of 'time'")
                                  }
                                }
                              } ~
                              get { request: HttpRequest[JValue] =>
                                tokenOf(request).flatMap { token =>
                                  val path        = fullPathOf(token, request)
                                  val observation = Obs.ofValue(variableOf(request), valueOf(request))
                                  val periodicity = periodicityOf(request)

                                  aggregationEngine.searchSeries(token, path, observation, periodicity)
                                  .map(_.serialize.ok)
                                }
                              }
                            }
                          }
                        } ~
                        path("geo") {
                          get { request: HttpRequest[JValue] =>
                            tokenOf(request).map { token =>
                              val path     = fullPathOf(token, request)
                              val variable = variableOf(request)

                              HttpResponse[JValue](content = None)
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
                    request.content.map[Future[HttpResponse[JValue]]] { content =>
                      val select = (content \ "select").deserialize[String].toLowerCase

                      val from: Path = token.path + "/" + (content \ "from").deserialize[String]

                      val observation = content.deserialize[Observation[HasValue]]

                      val start = (content \ "start") match {
                        case JNothing | JNull => None
                        case jvalue   => Some(jvalue.deserialize[Instant])
                      }

                      val end = (content \ "end") match {
                        case JNothing | JNull => None
                        case jvalue   => Some(jvalue.deserialize[Instant])
                      }

                      val grouping = (content \ "groupBy") match {
                        case JNothing | JNull => None
                        case jvalue   => Periodicity.byName(jvalue)
                      }

                      Selection(select) match {
                        case Count => 
                          aggregationEngine.searchCount(token, from, observation, start, end).map(_.serialize.ok)

                        case Series(periodicity) => 
                          aggregationEngine.searchSeries(token, from, observation, periodicity, start, end)
                          .map(groupTimeSeries(grouping))
                          .map(_.fold(_.serialize, _.serialize).ok)

                        case Related => 
                          import HttpStatusCodes.BadRequest
                          aggregationEngine.findRelatedInfiniteValues(
                            token, from, observation, 
                            start.getOrElse(throw new HttpException(BadRequest, "A start date must be specified to query for values related to an observation.")),
                            end.getOrElse(throw new HttpException(BadRequest, "An end date must be specified to query for values related to an observation."))
                          ).map(_.serialize.ok)
                      }
                    } getOrElse {
                      Future.sync(HttpResponse[JValue](content = None))
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
                    val content = request.content.get
                      
                    val from: Path = token.path + "/" + (content \ "from").deserialize[String]
                    val properties = (content \ "properties").deserialize[List[VariableDescriptor]]

                    val select = Selection((content \ "select").deserialize[String].toLowerCase)

                    val start = (content \ "start") match {
                      case JNothing | JNull => None
                      case jvalue   => Some(jvalue.deserialize[Instant])
                    }

                    val end = (content \ "end") match {
                      case JNothing | JNull => None
                      case jvalue   => Some(jvalue.deserialize[Instant])
                    }

                    val grouping = (content \ "groupBy") match {
                      case JNothing | JNull => None
                      case jvalue   => Periodicity.byName(jvalue)
                    }

                    select match {
                      case Count => 
                        aggregationEngine.intersectCount(token, from, properties, start, end)
                        .map(serializeIntersectionResult[CountType]).map(_.ok)

                      case Series(p) =>
                        aggregationEngine.intersectSeries(token, from, properties, p, start, end)
                        .map(_.map((groupTimeSeries(grouping)(_: TimeSeriesType).fold(_.serialize, _.serialize)).second)(collection.breakOut))
                        .map(serializeIntersectionResult[JValue]).map(_.ok)
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
                    val content: JValue = request.content.getOrElse("New token must be contained in POST content")

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
                          Future.sync(HttpResponse[JValue](status = HttpStatusCodes.Forbidden))
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
      case _ => throw HttpException(HttpStatusCodes.BadRequest, "Unknown or unspecified periodicity")
    }
  }

  def seriesGrouping(request: HttpRequest[_]): Option[Periodicity] = {
    request.parameters.get('groupBy).flatMap(Periodicity.byName)
  }

  def queryVariableSeries[T: Decomposer : AbelianGroup](tokenOf: HttpRequest[_] => Future[Token], f: ValueStats => T, aggregationEngine: AggregationEngine) = {
    getRange { (ranges, unit) => (request: HttpRequest[JValue]) =>
      tokenOf(request).flatMap { token =>
        val path        = fullPathOf(token, request)
        val variable    = variableOf(request)
        val periodicity = periodicityOf(request)

        unit.toLowerCase match {
          case "time" =>
            val (start, end) = ranges.head

            val startTime = new Instant(start)
            val endTime   = new Instant(end)

            aggregationEngine.getVariableSeries(token, path, variable, periodicity, Some(startTime), Some(endTime))
            .map(groupTimeSeries(seriesGrouping(request)))
            .map(_.fold(_.map(f).serialize, _.map(f).serialize).ok)

          case _ => throw HttpException(HttpStatusCodes.BadRequest, "GET with Range only accepts unit of 'time'")
        }
      }
    } ~
    get { request: HttpRequest[JValue] =>
      tokenOf(request).flatMap { token =>
        val path        = fullPathOf(token, request)
        val variable    = variableOf(request)
        val periodicity = periodicityOf(request)

        aggregationEngine.getVariableSeries(token, path, variable, periodicity)
        .map(_.map(f).serialize.ok)
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

  implicit val ObservationExtractor: Extractor[Observation[HasValue]] = new Extractor[Observation[HasValue]] {
    def extract(v: JValue): Observation[HasValue] = {
      (v \ "where").children.collect {
        case JField(name, value) =>
          val variable  = Variable(JPath(name))
          val predicate = HasValue(value)

          (variable -> predicate)
      }.toSet
    }
  }
}
