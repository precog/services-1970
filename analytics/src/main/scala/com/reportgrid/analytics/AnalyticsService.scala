package com.reportgrid
package analytics

import blueeyes._
import blueeyes.concurrent.Future
import blueeyes.core.data.{BijectionsChunkJson, BijectionsChunkString}
import blueeyes.core.http._
import blueeyes.core.http.MimeTypes.{application, json}
import blueeyes.core.service._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.{JPath, JsonParser, JPathField}
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultOrderings._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.JodaSerializationImplicits._
import blueeyes.persistence.mongo._
import blueeyes.persistence.cache.{Stage, ExpirationPolicy, CacheSettings}
import blueeyes.util.{Clock, ClockSystem, PartialFunctionCombinators}
import HttpStatusCodes.{BadRequest, Unauthorized, Forbidden}

import net.lag.configgy.{Configgy, ConfigMap}

import org.joda.time.base.AbstractInstant
import org.joda.time.Instant
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import java.net.URL
import java.util.concurrent.TimeUnit

import scala.collection.immutable.SortedMap
import scala.collection.immutable.IndexedSeq

import scalaz.Monoid
import scalaz.Validation
import scalaz.Success
import scalaz.Failure
import scalaz.NonEmptyList
import scalaz.Scalaz._

import com.reportgrid.analytics.external._
import com.reportgrid.ct._
import com.reportgrid.ct.Mult._
import com.reportgrid.ct.Mult.MDouble._

import com.reportgrid.instrumentation.blueeyes.ReportGridInstrumentation
import com.reportgrid.api.ReportGridTrackingClient

case class AnalyticsState(aggregationEngine: AggregationEngine, tokenManager: TokenManager, clock: Clock, auditClient: ReportGridTrackingClient[JValue], yggdrasil: Yggdrasil[JValue], jessup: Jessup)

trait AnalyticsService extends BlueEyesServiceBuilder with BijectionsChunkJson with BijectionsChunkString with ReportGridInstrumentation {
  import AggregationEngine._
  import AnalyticsService._
  import AnalyticsServiceSerialization._

  def mongoFactory(configMap: ConfigMap): Mongo

  def auditClient(configMap: ConfigMap): ReportGridTrackingClient[JValue] 

  def yggdrasil(configMap: ConfigMap): Yggdrasil[JValue]

  def jessup(configMap: ConfigMap): Jessup

  val analyticsService = service("analytics", "1.0") {
    logging { logger =>
      healthMonitor { monitor => context =>
        startup {
          import context._

          val mongoConfig = config.configMap("mongo")

          val mongo = mongoFactory(mongoConfig)

          val database = mongo.database(mongoConfig.getString("database", "analytics-v" + serviceVersion))

          val tokensCollection = mongoConfig.getString("tokensCollection", "tokens")

          for {
            tokenManager      <- TokenManager(database, tokensCollection)
            aggregationEngine <- AggregationEngine(config, logger, database)
          } yield {
            AnalyticsState(
              aggregationEngine, tokenManager, ClockSystem.realtimeClock, 
              auditClient(config.configMap("audit")),
              yggdrasil(config.configMap("yggdrasil")),
              jessup(config.configMap("jessup")))
          }
        } ->
        request { (state: AnalyticsState) =>
          import state.{aggregationEngine, tokenManager}

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

          val audit = auditor[JValue, JValue](state.auditClient, state.clock, tokenOf)

          def aggregate(request: HttpRequest[JValue], tagExtractors: List[Tag.TagExtractor]) = {
            val count: Int = request.parameters.get('count).map(_.toInt).getOrElse(1)

            withTokenAndPath(request) { (token, path) => 
              logger.debug(count + "|" + token.tokenId + "|" + path.path + "|" + request.content.map(o => compact(render(o))))
              request.content.foreach { 
                case obj @ JObject(fields) => for (JField(eventName, event: JObject) <- fields) {
                  aggregationEngine.store(token, path, eventName, event, count)

                  val (tagResults, remainder) = Tag.extractTags(tagExtractors, event)
                  for (tags <- getTags(tagResults)) {
                    aggregationEngine.aggregate(token, path, eventName, tags, remainder, count)
                  }
                }

                case err => 
                  throw new HttpException(BadRequest, "Expected a JSON object but got " + pretty(render(err)))
              }

              Future.sync(HttpResponse[JValue](content = None))
            }
          }

          jsonp {
            /* The virtual file system, which is used for storing data,
             * retrieving data, and querying for metadata.
             */
            path("""/store/vfs/(?:(?<prefixPath>(?:[^\n.](?:[^\n/]|/[^\n\.])+)/?)?)""") { 
              $ {
                audit("store") {
                  state.yggdrasil {
                    post { request: HttpRequest[JValue] =>
                      val tagExtractors = Tag.timeTagExtractor(timeSeriesEncoding, state.clock.instant(), false) ::
                                          Tag.locationTagExtractor(state.jessup(request.remoteHost))      :: Nil

                      aggregate(request, tagExtractors)
                    }
                  }
                }
              }
            } ~
            path("""/vfs/(?:(?<prefixPath>(?:[^\n.](?:[^\n/]|/[^\n\.])+)/?)?)""") { 
              $ {
                /* Post data to the virtual file system.
                 */
                audit("track") {
                  state.yggdrasil {
                    post { request: HttpRequest[JValue] =>
                      val tagExtractors = Tag.timeTagExtractor(timeSeriesEncoding, state.clock.instant(), true) ::
                                          Tag.locationTagExtractor(state.jessup(request.remoteHost))      :: Nil

                      aggregate(request, tagExtractors)
                    }
                  }
                } ~
                audit("explore paths") {
                  get { request: HttpRequest[JValue] =>
                    withTokenAndPath(request) { (token, path) => 
                      if (token.permissions.explore) {
                        aggregationEngine.getPathChildren(token, path).map(_.serialize.ok)
                      } else {
                        throw new HttpException(Unauthorized, "The specified token does not permit exploration of the virtual filesystem.")
                      }
                    }
                  }
                }
              } ~
              path("""(?<variable>\.[^\n/]+)""") {
                $ {
                  //audit("explore variables") {
                    get { request: HttpRequest[JValue] =>
                      val variable = variableOf(request)

                      withTokenAndPath(request) { (token, path) => 
                        if (token.permissions.explore) {
                          aggregationEngine.getVariableChildren(token, path, variable).map(_.map(_.child).serialize.ok)
                        } else {
                          throw new HttpException(Unauthorized, "The specified token does not permit exploration of the virtual filesystem.")
                        }
                      }
                    }
                  //}
                } ~
                path("/") {
                  path("count") {
                    audit("variable occurrence count") { request: HttpRequest[JValue] =>
                      //post { request: HttpRequest[JValue] =>
                        val variable = variableOf(request)

                        withTokenAndPath(request) { (token, path) => 
                          val terms = List(timeSpanTerm, locationTerm).flatMap(_.apply(request.parameters, request.content))
                          aggregationEngine.getVariableCount(token, path, variable, terms).map(_.serialize.ok)
                        }
                      //}
                    }
                  } ~
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
                  path("histogram") {
                    $ {
                      audit("variable histogram") {
                        get { request: HttpRequest[JValue] =>
                          val variable = variableOf(request)

                          withTokenAndPath(request) { (token, path) => 
                            aggregationEngine.getHistogram(token, path, variable).map(_.serialize.ok)
                          }
                        }
                      }
                    } ~
                    path("/") {
                      path("top/'limit") {
                        audit("variable histogram top") {
                          get { request: HttpRequest[JValue] =>
                            val variable = variableOf(request)
                            val limit    = request.parameters('limit).toInt

                            withTokenAndPath(request) { (token, path) => 
                              aggregationEngine.getHistogramTop(token, path, variable, limit).map(_.serialize.ok)
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
                              aggregationEngine.getHistogramBottom(token, path, variable, limit).map(_.serialize.ok)
                            }
                          }
                        }
                      }
                    }
                  } ~
                  path("length") {
                    audit("count of variable values") {
                      get { request: HttpRequest[JValue] =>
                        val variable = variableOf(request)

                        withTokenAndPath(request) { (token, path) => 
                          aggregationEngine.getVariableLength(token, path, variable).map(_.serialize.ok)
                        }
                      }
                    }
                  } ~
                  path("values") {
                    $ {
                      audit("list of variable values") {
                        get { request: HttpRequest[JValue] =>
                          val variable = variableOf(request)

                          withTokenAndPath(request) { (token, path) => 
                            if (token.permissions.explore) {
                              aggregationEngine.getValues(token, path, variable).map(_.toList.serialize.ok)
                            } else {
                              throw new HttpException(Unauthorized, "The specified token does not permit exploration of the virtual filesystem.")
                            }
                          }
                        }
                      }
                    } ~
                    path("/") {
                      path("top/'limit") {
                        audit("list of top variable values") {
                          get { request: HttpRequest[JValue] =>
                            val variable = variableOf(request)
                            val limit    = request.parameters('limit).toInt

                            withTokenAndPath(request) { (token, path) => 
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
                            audit("count occurrences of a variable value") { request: HttpRequest[JValue] =>
                              //post { request: HttpRequest[JValue] =>
                                val observation = JointObservation(HasValue(variableOf(request), valueOf(request)))

                                withTokenAndPath(request) { (token, path) => 
                                  val terms = List(timeSpanTerm, locationTerm).flatMap(_.apply(request.parameters, request.content))
                                  aggregationEngine.getObservationCount(token, path, observation, terms) map (_.serialize.ok)
                                }
                              //}
                            }
                          } ~
                          path("series/") {
                            get { request: HttpRequest[JValue] =>
                              // simply return the names of valid periodicities that can be used for series queries
                              Future.sync(JArray(Periodicity.Default.map(p => JString(p.name))).ok[JValue])
                            } ~
                            path('periodicity) { 
                              audit("variable value series") { request: HttpRequest[JValue] =>
                                //post { request: HttpRequest[JValue] =>
                                  val periodicity = periodicityOf(request)
                                  val observation = JointObservation(HasValue(variableOf(request), valueOf(request)))
                                  val terms = List(intervalTerm(periodicity), locationTerm).flatMap(_.apply(request.parameters, request.content))
                                  withTokenAndPath(request) { (token, path) => 
                                    aggregationEngine.getObservationSeries(token, path, observation, terms)
                                    .map(transformTimeSeries(request, periodicity))
                                    .map(_.serialize.ok)
                                  }
                                //} 
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
                      throw new HttpException(BadRequest, """request body was empty. "select", "from", and "where" fields must be specified.""")
                    }
                      
                    val queryComponents = (content \ "select").validated[String].map(Selection(_)).liftFailNel |@| 
                                          (content \ "from").validated[String].map(token.path / _).liftFailNel |@|
                                          (content \ "where").validated[Set[HasValue]].map(JointObservation(_)).liftFailNel

                    val result = queryComponents.apply {
                      (select, from, observation) => select match {
                        case Count => 
                          val terms = List(timeSpanTerm, locationTerm).flatMap(_.apply(request.parameters, request.content))
                          aggregationEngine.getObservationCount(token, from, observation, terms).map(_.serialize.ok)

                        case Series(periodicity) => 
                          val terms = List(intervalTerm(periodicity), locationTerm).flatMap(_.apply(request.parameters, request.content))
                          aggregationEngine.getObservationSeries(token, from, observation, terms)
                          .map(transformTimeSeries(request, periodicity))
                          .map(_.serialize.ok)

                        case Related => 
                          timeSpan(request.parameters, Some(content)) match {
                            case Some(Success(span)) => aggregationEngine.findRelatedInfiniteValues(token, from, observation, span) map (_.serialize.ok)
                            case Some(Failure(errors)) => throw new HttpException(BadRequest, errors.list.mkString("; "))
                            case None => throw new HttpException(BadRequest, "A time span must be specified for related values queries.")
                          }
                      }
                    }

                    result.fold(errors => throw new HttpException(BadRequest, errors.list.mkString("; ")), success => success)
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
                      
                    val queryComponents = (content \ "select").validated[String].map(Selection(_)).liftFailNel |@| 
                                          (content \ "from").validated[String].map(token.path / _).liftFailNel |@|
                                          (content \ "properties").validated[List[VariableDescriptor]].liftFailNel


                    val result = queryComponents.apply {
                      case (select, from, where) => select match {
                        case Count => 
                          val terms = List(timeSpanTerm, locationTerm).flatMap(_.apply(request.parameters, request.content))
                          aggregationEngine.getIntersectionCount(token, from, where, terms)
                          .map(serializeIntersectionResult[CountType]).map(_.ok)

                        case Series(periodicity) =>
                          val terms = List(intervalTerm(periodicity), locationTerm).flatMap(_.apply(request.parameters, request.content))
                          aggregationEngine.getIntersectionSeries(token, from, where, terms)
                          .map(_.map(transformTimeSeries[CountType](request, periodicity).second))
                          .map(serializeIntersectionResult[ResultSet[JObject, CountType]]).map(_.ok)
                      }
                    }

                    result.fold(errors => throw new HttpException(BadRequest, errors.list.mkString("; ")), success => success)
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
                  tokenOf(request).flatMap { parent =>
                    val content: JValue = request.content.getOrElse {
                      throw new HttpException(BadRequest, "New token must be contained in POST content")
                    }

                    val path        = (content \ "path").deserialize[Option[String]].getOrElse("/")
                    val permissions = (content \ "permissions").deserialize(permissionsExtractor(parent.permissions))
                    val expires     = (content \ "expires").deserialize[Option[DateTime]].getOrElse(parent.expires)
                    val limits      = (content \ "limits").deserialize(limitsExtractor(parent.limits))

                    if (expires < state.clock.now()) {
                      throw new HttpException(BadRequest, "Your are attempting to create an expired token. Such a token will not be usable.")
                    } else {
                      tokenManager.issueNew(parent, path, permissions, expires, limits) map {
                        case Success(newToken) => HttpResponse[JValue](content = Some(newToken.tokenId.serialize))
                        case Failure(message) => throw new HttpException(BadRequest, message)
                      }
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

  type Endo[A] = A => A

  def validated[A](v: Option[ValidationNEL[String, A]]): Option[A] = v map {
    case Success(a) => a
    case Failure(t) => throw new HttpException(BadRequest, t.list.mkString("; "))
  }

  def transformTimeSeries[V: AbelianGroup : MDouble](request: HttpRequest[_], periodicity: Periodicity): Endo[ResultSet[JObject, V]] = {
    val zone = validated(timezone(request.parameters))
    shiftTimeSeries[V](periodicity, zone) andThen groupTimeSeries[V](periodicity, seriesGrouping(request), zone)
  }

  val TimeOrder: Ordering[JObject] = new Ordering[JObject] {
    override def compare(k1: JObject, k2: JObject) = {
      (timeField(k1) |@| timeField(k2)) { _ compare _ } getOrElse 0
    }
  }

  val timeField = (o: JObject) => {
    o.fields.collect { case JField("timestamp", t) => t.deserialize[Instant] }.headOption
  }

  def shiftTimeField = (obj: JObject, zone: DateTimeZone) => JObject(
    obj.fields.map {
      case JField("timestamp", instant) => JField("datetime", instant.deserialize[Instant].toDateTime(DateTimeZone.UTC).withZone(zone).toString)

      case field => field
    }
  )
    
  def shiftTimeSeries[V: AbelianGroup : MDouble](periodicity: Periodicity, zone: Option[DateTimeZone]): Endo[ResultSet[JObject, V]] = {
    import SAct._

    (resultSet: ResultSet[JObject, V]) => {
      val shifted = zone filter (_ != DateTimeZone.UTC) map { zone => 
        // check whether the offset from UTC with respect to the periodicity is fractional - if so, we will need
        // to interpolate.

        val offsetFraction = resultSet.headOption.map(_._1) >>= timeField >>= (periodicity.offsetFraction(zone, _))
        if (offsetFraction.exists(_ == 0.0)) {
          // no partial offset from UTC is necessary - this will be the case when using a periodicity such as 
          // hours or minutes and a time zone that is shifted in hour increments.
          resultSet.map((shiftTimeField(_: JObject, zone)).first)
        } else {
          // Use cubic interpolation to interpolate values; this makes the assumption (based upon the behavior
          // of AggregationEngine) that series are regularly spaced in time.
          val (_, results) = resultSet.sortBy(_._1)(TimeOrder).foldLeft((mzero[V], Vector.empty[(JObject, V)])) { 
            case ((rem, results), (k, v)) => 
               val adjustments = for (time <- timeField(k); offset <- periodicity.offsetFraction(zone, time)) yield {
                // The time zone offset fraction needs to be adjusted to be a value between 0 and 1
                // for the purposes of interpolation. 
                if (offset > 0) {
                  // time series is being shifted into the past
                  (v |+| offset, v |+| (1.0 - offset))
                } else {
                  // time series is being shifted into the future
                  (v |+| (1.0d + offset), v |+| -offset)
                }
              }

              val (resultValue, remainder) = adjustments.getOrElse((v, mzero[V]))
              (remainder, results :+ ((shiftTimeField(k, zone), resultValue |+| rem)))
          }

          results
        }
      } getOrElse {
        resultSet
      }
      
      if (shifted.isEmpty) shifted else shifted.tail //since intervalTerm extends the query interval back in time
    }
  }

  def groupTimeSeries[V: Monoid](original: Periodicity, grouping: Option[(Periodicity, Set[Int])], zone: Option[DateTimeZone]) = (resultSet: ResultSet[JObject, V]) => {
    grouping match {
      case Some((grouping, groups)) => 
        def newField(dateTime: DateTime) = {
          val index = original.indexOf(dateTime, grouping).getOrElse {
            sys.error("Cannot group time series of periodicity " + original + " by " + grouping)
          }
          
          (groups.isEmpty || groups.contains(index)) option JField(original.name, index)
        }

        resultSet.foldLeft(SortedMap.empty[JObject, V](JObjectOrdering)) {
          case (acc, (obj, v)) => 
            val key = JObject(
              obj.fields.flatMap {
                case JField("datetime",  datetime) => newField(datetime.deserialize[DateTime])
                case JField("timestamp", instant)  => newField(instant.deserialize[Instant].toDateTime(DateTimeZone.UTC))
                  
                case field => Some(field)
              })

            acc + (key -> acc.get(key).map(_ |+| v).getOrElse(v))
        }.toSeq

      case None => resultSet
    }
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

  def seriesGrouping(request: HttpRequest[_]): Option[(Periodicity, Set[Int])] = {
    request.parameters.get('groupBy).flatMap(Periodicity.byName).map { grouping =>
      (grouping, request.parameters.get('groups).toSet.flatMap((_:String).split(",").map(_.toInt)))
    }
  }

  def dateTimeZone(s: String): ValidationNEL[String, DateTimeZone] = {
    val Offset = """([+-]?\d{1,2})(?:\.(\d+))?""".r
    s match {
      case Offset(hoursText, minutesText) => 
        val hours = hoursText.replaceAll("\\+", "").parseInt.fail.map(t => ("Error encountered parsing hours component of time zone: " + t.getMessage).wrapNel).validation

        val minutes = Option(minutesText).map { m => 
          ("0."+m).parseFloat.map(f => (f * 60).toInt).fail.map(t => ("Error encountered parsing fractional component of time zone: " + t.getMessage).wrapNel).validation
        }

        minutes match {
          case Some(m) => (hours |@| m).apply(DateTimeZone.forOffsetHoursMinutes)
          case None =>     hours.map(DateTimeZone.forOffsetHours)
        }

      case id => 
        try {
          DateTimeZone.forID(s).success
        } catch {
          case ex => ex.getMessage.fail[DateTimeZone].liftFailNel
        }
    }
  }

  def timezone(parameters: Map[Symbol, String]): Option[ValidationNEL[String, DateTimeZone]] = {
    parameters.get('timeZone).map(dateTimeZone)
  }

  def vtry[A](value: => A): Validation[Throwable, A] = try { value.success } catch { case ex => ex.fail[A] }

  def timeSpan(parameters: Map[Symbol, String], content: Option[JValue]): Option[ValidationNEL[String, TimeSpan]] = {
    val zone = timezone(parameters).getOrElse(DateTimeZone.UTC.success.liftFailNel)

    def parseDate(s: String) = zone.flatMap { z => 
      // convert to a DateTime object in the specified zone. If not specified, use UTC.
      val timestamp = s.parseLong.map(new DateTime(_: Long, z)).fail.map(_.getMessage.wrapNel).validation
      timestamp <+> vtry(new DateTime(s, z)).fail.map(_.getMessage.wrapNel).validation
    }

    def extractDate(name: String) = content.flatMap(_ \? name).map { timejv => 
      zone.map(ZonedTimeExtractor).flatMap { ex => 
        timejv.validated[DateTime](ex).fail.map(_.message.wrapNel).validation
      }
    }

    val start = parameters.get('start) map parseDate orElse extractDate("start")
    val end =   parameters.get('end)   map parseDate orElse extractDate("end")

    val shift = (_: DateTime).withZoneRetainFields(DateTimeZone.UTC).toInstant
    for (sv <- start; ev <- end) yield { 
      (sv <**> ev) { 
        (s, e) => TimeSpan(shift(s), shift(e))
      }
    }
  }

  type TermF = (Map[Symbol, String], Option[JValue]) => Option[TagTerm]

  val timeSpanTerm: TermF = (parameters: Map[Symbol, String], content: Option[JValue]) => {
    validated(timeSpan(parameters, content)).map(SpanTerm(timeSeriesEncoding, _))
  }

  def intervalTerm(periodicity: Periodicity): TermF = (parameters: Map[Symbol, String], content: Option[JValue]) => {
    validated(timeSpan(parameters, content)).map(IntervalTerm(timeSeriesEncoding, periodicity, _).extendForInterpolation)
  }

  val locationTerm: TermF = (parameters: Map[Symbol, String], content: Option[JValue]) => {
    parameters.get('location) map (p => Hierarchy.AnonLocation(Path(p))) orElse {
      content.map(_ \ "location").flatMap { 
        case JNothing | JNull => None
        case jvalue => Some(jvalue.deserialize[Hierarchy.Location])
      }
    } map (HierarchyLocationTerm("location", _))
  }

  def queryVariableSeries[T: Decomposer : AbelianGroup](tokenOf: HttpRequest[_] => Future[Token], f: ValueStats => T, aggregationEngine: AggregationEngine) = {
    //post { request: HttpRequest[JValue] =>
      (request: HttpRequest[JValue]) => tokenOf(request).flatMap { token =>
        val path        = fullPathOf(token, request)
        val variable    = variableOf(request)
        val periodicity = periodicityOf(request)
        val terms = List(intervalTerm(periodicity), locationTerm).flatMap(_.apply(request.parameters, request.content))

        aggregationEngine.getVariableSeries(token, path, variable, terms) 
        .map(transformTimeSeries[ValueStats](request, periodicity))
        .map(_.map(f.second).serialize.ok)
      }
    //} 
  }

  def getTags(result: Tag.ExtractionResult) = result match {
    case Tag.Tags(tags) => tags
    case Tag.Skipped => Future.sync(Nil)
    case Tag.Errors(errors) =>
      val errmsg = "Errors occurred extracting tag information: " + errors.map(_.toString).mkString("; ")
      throw new HttpException(BadRequest, errmsg)
  }
}

object AnalyticsServiceSerialization extends AnalyticsSerialization {
  import AggregationEngine._

  // Decomposer is invariant, which means that this can't be reasonably implemented
  // as a Decomposer and used both for intersection results and grouped intersection
  // results.
  def serializeIntersectionResult[T: Decomposer](result: Seq[(JArray, T)]) = {
    result.foldLeft[JValue](JObject(Nil)) {
      case (result, (key, value)) => 
        result.set(JPath(key.elements.map(v => JPathField(renderNormalized(v)))), value.serialize)
    }
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

  implicit val VariableDescriptorExtractor: Extractor[VariableDescriptor] = new Extractor[VariableDescriptor] with ValidatedExtraction[VariableDescriptor] {
    override def validated(jvalue: JValue) = {
      ((jvalue \ "property").validated[Variable] |@| (jvalue \ "limit").validated[Int] |@| (jvalue \ "order").validated[SortOrder]).apply(VariableDescriptor(_, _, _))
    }
  }
}
