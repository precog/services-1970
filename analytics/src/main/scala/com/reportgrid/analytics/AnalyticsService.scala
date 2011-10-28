package com.reportgrid.analytics

import com.reportgrid.analytics.service._

import blueeyes._
import blueeyes.concurrent.Future
import blueeyes.core.data.{BijectionsChunkJson, BijectionsChunkFutureJson, BijectionsChunkString, ByteChunk}
import blueeyes.core.http._
import blueeyes.core.http.MimeTypes.{application, json}
import blueeyes.core.service._
import blueeyes.core.service.RestPathPattern._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.{JPath, JsonParser, JPathField}
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultOrderings._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.JodaSerializationImplicits._
import blueeyes.persistence.mongo._
import blueeyes.persistence.cache.{Stage, ExpirationPolicy, CacheSettings}
import blueeyes.util.{Clock, ClockSystem, PartialFunctionCombinators, InstantOrdering}
import scala.math.Ordered._
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

case class AnalyticsState(aggregationEngine: AggregationEngine, tokenManager: TokenManager, auditClient: ReportGridTrackingClient[JValue], jessup: Jessup)

trait AnalyticsService extends BlueEyesServiceBuilder with AnalyticsServiceCombinators with ReportGridInstrumentation {
  import AggregationEngine._
  import AnalyticsService._
  import AnalyticsServiceSerialization._
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  def mongoFactory(configMap: ConfigMap): Mongo

  def auditClient(configMap: ConfigMap): ReportGridTrackingClient[JValue] 

  def jessup(configMap: ConfigMap): Jessup

  val clock: Clock

  val analyticsService = this.service("analytics", "1.0") {
    requestLogging {
    logging { logger =>
      healthMonitor { monitor => context =>
        startup {
          import context._

          val eventsdbConfig = config.configMap("eventsdb")
          val eventsMongo = mongoFactory(eventsdbConfig)
          val eventsdb = eventsMongo.database(eventsdbConfig.getString("database", "events-v" + serviceVersion))

          val indexdbConfig = config.configMap("indexdb")
          val indexMongo = mongoFactory(indexdbConfig)
          val indexdb  = indexMongo.database(indexdbConfig.getString("database", "analytics-v" + serviceVersion))

          val tokensCollection = config.getString("tokens.collection", "tokens")

          for {
            tokenManager      <- TokenManager(indexdb, tokensCollection)
            aggregationEngine <- AggregationEngine(config, logger, eventsdb, indexdb)
          } yield {
            AnalyticsState(
              aggregationEngine, tokenManager, 
              auditClient(config.configMap("audit")),
              jessup(config.configMap("jessup")))
          }
        } ->
        request { (state: AnalyticsState) =>
          import state.{aggregationEngine, tokenManager}

          val audit = auditor(state.auditClient, clock, tokenManager)
          import audit._

          jsonp[ByteChunk] {
            token(tokenManager) {
              /* The virtual file system, which is used for storing data,
               * retrieving data, and querying for metadata.
               */
              path("/store") {
                vfsPath {
                  post(new TrackingService(aggregationEngine, timeSeriesEncoding, clock, state.jessup, false)).audited("store")
                }
              } ~ 
              vfsPath {
                post(new TrackingService(aggregationEngine, timeSeriesEncoding, clock, state.jessup, true)).audited("track") ~
                get(new ExplorePathService[Future[JValue]](aggregationEngine)).audited("explore paths") ~
                variable {
                  path(/?) {
                    get {
                      new ExploreVariableService[Future[JValue]](aggregationEngine).audited("explore variable children") 
                    }
                  } ~
                  path("/") { 
                    commit {
                      path("count") {
                        audited("count occurrences of a variable") {
                          (request: HttpRequest[Future[JValue]]) => {
                            (token: Token, path: Path, variable: Variable) => {
                              val futureContent: Future[Option[JValue]] = request.content.map(_.map[Option[JValue]](Some(_)))
                                                                                 .getOrElse(Future.sync[Option[JValue]](None))
                              futureContent.flatMap { content => 
                                val terms = List(timeSpanTerm, locationTerm).flatMap(_.apply(request.parameters, content))
                                aggregationEngine.getVariableCount(token, path, variable, terms).map(_.serialize.ok)
                              }
                            }
                          }
                        }
                      } ~ 
                      path("statistics") {
                        get { 
                          audited("variable statistics") {
                            (_: HttpRequest[Future[JValue]]) => 
                              aggregationEngine.getVariableStatistics(_: Token, _: Path, _: Variable).map(_.serialize.ok)
                          }
                        }
                      } ~
                      path("series") {
                        commit {
                          get { 
                            // simply return the names of valid periodicities that can be used for series queries
                            (_: HttpRequest[Future[JValue]]) => (_: Token, _: Path, _: Variable) => 
                              Future.sync(JArray(Periodicity.Default.map(p => JString(p.name))).ok[JValue])
                          } ~
                          path("/'periodicity") {
                            path(/?) {
                              get {
                                new VariableSeriesService(aggregationEngine, _.count).audited("variable count series")
                              }
                            } ~ 
                            path("/") {
                              path("means") {
                                new VariableSeriesService(aggregationEngine, _.mean).audited("variable mean series")
                              } ~ 
                              path("standardDeviations") {
                                new VariableSeriesService(aggregationEngine, _.standardDeviation).audited("variable mean series")
                              } 
                            }
                          }
                        }
                      } ~ 
                      path("histogram") {
                        path(/?) {
                          get { 
                            audited("Return a histogram of values of a variable.") { 
                              (_: HttpRequest[Future[JValue]]) => aggregationEngine.getHistogram(_: Token, _: Path, _: Variable).map(_.serialize.ok)
                            }
                          }
                        } ~
                        commit {
                          path("/") {
                            pathData("top/'limit", 'limit, parsePathInt("limit")) {
                              get { 
                                audited("Return a histogram of the top 'limit values of a variable, by count.") { 
                                  (_: HttpRequest[Future[JValue]]) => 
                                    (limit: Int) => aggregationEngine.getHistogramTop(_: Token, _: Path, _: Variable, limit).map(_.serialize.ok) 
                                }
                              }
                            } ~
                            pathData("bottom/'limit", 'limit, parsePathInt("limit")) {
                              get { 
                                audited("Return a histogram of the bottom 'limit values of a variable, by count.") { 
                                  (_: HttpRequest[Future[JValue]]) => 
                                    (limit: Int) => aggregationEngine.getHistogramBottom(_: Token, _: Path, _: Variable, limit).map(_.serialize.ok)
                                }
                              }
                            }
                          }
                        }
                      } ~
                      path("length") {
                        get { 
                          audited("Count the number of values for a variable.") {
                            (_: HttpRequest[Future[JValue]]) => 
                              aggregationEngine.getVariableLength(_: Token, _: Path, _: Variable).map(_.serialize.ok)
                          }
                        }
                      } ~
                      path("values") {
                        path(/?) {
                          get { 
                            new ExploreValuesService[Future[JValue]](aggregationEngine).audited("list of variable values") 
                          }
                        } ~
                        path("/") {
                          commit {
                            pathData("top/'limit", 'limit, parsePathInt("limit")) {
                              get { 
                                audited("List the top n variable values") {
                                  (_: HttpRequest[Future[JValue]]) => 
                                    (limit: Int) => aggregationEngine.getValuesTop(_: Token, _: Path, _: Variable, limit).map(_.serialize.ok)
                                }
                              }
                            } ~
                            pathData("bottom/'limit", 'limit, parsePathInt("limit")) {
                              get { 
                                audited("list of bottom variable values") {
                                  (_: HttpRequest[Future[JValue]]) => 
                                    (limit: Int) => aggregationEngine.getValuesBottom(_: Token, _: Path, _: Variable, limit).map(_.serialize.ok)
                                }
                              }
                            } ~
                            pathData('value, 'value, ((e: Exception) => DispatchError(HttpException(BadRequest, e))) <-: JsonParser.parseValidated(_)) {
                              path(/?) {
                                get { 
                                  // return a list of valid subpaths
                                  (_: HttpRequest[Future[JValue]]) => (_: JValue) => (_: Token, _: Path, _: Variable) => 
                                    Future.sync(JArray(JString("count") :: JString("series/") :: Nil).ok[JValue])
                                }
                              } ~
                              path("/") {
                                commit {
                                  path("count") {
                                    get {
                                      audited("count occurrences of a variable value") { 
                                        (request: HttpRequest[Future[JValue]]) => (value: JValue) => (token: Token, path: Path, variable: Variable) => {
                                          val futureContent: Future[Option[JValue]] = request.content.map(_.map[Option[JValue]](Some(_)))
                                                                                             .getOrElse(Future.sync[Option[JValue]](None))

                                          futureContent.flatMap { content => 
                                            val terms = List(timeSpanTerm, locationTerm).flatMap(_.apply(request.parameters, content))
                                            aggregationEngine.getObservationCount(token, path, JointObservation(HasValue(variable, value)), terms)
                                            .map(_.serialize.ok)
                                          }
                                        }
                                      }
                                    }
                                  } ~
                                  path("series") {
                                    get { 
                                      // simply return the names of valid periodicities that can be used for series queries
                                      (_: HttpRequest[Future[JValue]]) => (_: JValue) => (_: Token, _: Path, _: Variable) => 
                                        Future.sync(JArray(Periodicity.Default.map(p => JString(p.name))).ok[JValue])
                                    } ~
                                    path("/'periodicity") { 
                                      new ValueSeriesService(aggregationEngine).audited("Returns a time series of variable values.")
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
                }
              } ~
              path("/search") {
                post { 
                  new SearchService(aggregationEngine).audited("Adavanced count and series queries.") 
                }
              } ~
              path("/intersect") {
                post { 
                  new IntersectionService(aggregationEngine).audited("intersection query")
                } 
              } ~ 
              path("/tokens") {
                commit {
                  path(/?) {
                    get { 
                      (request: HttpRequest[Future[JValue]]) => (token: Token) => {
                        tokenManager.listDescendants(token) map { 
                          _.map(_.tokenId).serialize.ok
                        }
                      }
                    } ~
                    post { 
                      (request: HttpRequest[Future[JValue]]) => (parent: Token) => {
                        request.content map { 
                          _.flatMap { content => 
                            val path        = (content \ "path").deserialize[Option[String]].getOrElse("/")
                            val permissions = (content \ "permissions").deserialize(Permissions.permissionsExtractor(parent.permissions))
                            val expires     = (content \ "expires").deserialize[Option[DateTime]].getOrElse(parent.expires)
                            val limits      = (content \ "limits").deserialize(Limits.limitsExtractor(parent.limits))

                            if (expires < clock.now()) {
                              Future.sync(HttpResponse[JValue](BadRequest, content = Some("Your are attempting to create an expired token. Such a token will not be usable.")))
                            } else tokenManager.issueNew(parent, path, permissions, expires, limits) map {
                              case Success(newToken) => HttpResponse[JValue](content = Some(newToken.tokenId.serialize))
                              case Failure(message) => throw new HttpException(BadRequest, message)
                            }
                          }
                        } getOrElse {
                          Future.sync(HttpResponse[JValue](BadRequest, content = Some("New token must be contained in POST content")))
                        }
                      }
                    }
                  } ~
                  path("/") {
                    path('descendantTokenId) {
                      get { 
                        (request: HttpRequest[Future[JValue]]) => (token: Token) => {
                          if (token.tokenId == request.parameters('descendantTokenId)) {
                            token.parentTokenId.map { parTokenId =>
                              tokenManager.lookup(parTokenId).map { parent => 
                                val sanitized = parent.map(token.relativeTo).map(_.copy(parentTokenId = None, accountTokenId = ""))
                                HttpResponse[JValue](content = sanitized.map(_.serialize))
                              }
                            } getOrElse {
                              Future.sync(HttpResponse[JValue](Forbidden))
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
                      delete { 
                        (request: HttpRequest[Future[JValue]]) => (token: Token) => {
                          tokenManager.deleteDescendant(token, request.parameters('descendantTokenId)).map { _ =>
                            HttpResponse[JValue](content = None)
                          }
                        }
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
  }}
}

object AnalyticsService extends HttpRequestHandlerCombinators with PartialFunctionCombinators {
  import AnalyticsServiceSerialization._
  import AggregationEngine._

  type Endo[A] = A => A

  def parsePathInt(name: String) = 
    ((err: NumberFormatException) => DispatchError(BadRequest, "Illegal value for path parameter " + name + ": " + err.getMessage)) <-: (_: String).parseInt

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
