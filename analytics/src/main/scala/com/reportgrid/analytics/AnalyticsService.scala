package com.reportgrid.analytics

import com.reportgrid.analytics.service._

import blueeyes._
import blueeyes.bkka._
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

import com.weiglewilczek.slf4s.Logging

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Duration
import org.joda.time.Instant
import org.joda.time.base.AbstractInstant

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

case class AnalyticsState(insertEventsMongo: Mongo, queryEventsMongo: Mongo, indexMongo: Mongo, aggregationEngine: AggregationEngine, tokenManager: TokenManager, storageReporting: StorageReporting, auditClient: ReportGridTrackingClient[JValue], jessup: Jessup, defaultRawLimit: Int)

trait AnalyticsService extends BlueEyesServiceBuilder with AnalyticsServiceCombinators with ReportGridInstrumentation {
  import AggregationEngine._
  import AnalyticsService._
  import AnalyticsServiceSerialization._
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._


  def mongoFactory(configMap: ConfigMap): Mongo

  def auditClient(configMap: ConfigMap): ReportGridTrackingClient[JValue] 

  def storageReporting(configMap: ConfigMap): StorageReporting

  def jessup(configMap: ConfigMap): Jessup

  def tokenManager(database: Database, tokensCollection: MongoCollection, deletedTokensCollection: MongoCollection): TokenManager

  val clock: Clock
 
  // not easy to have configuration at outer levels so hardwiring implicit timeout
  // for requestLogging, healthMonitor, etc
  implicit val timeout = akka.actor.Actor.Timeout(120000) 

  val analyticsService = this.service("analytics", "1.0") {
    requestLogging {
    logging { logger =>
      healthMonitor { monitor => context =>
        startup {
          import context._
          
          val eventsdbConfig = config.configMap("eventsdb")

          val insertEventsMongo = mongoFactory(eventsdbConfig)
          val insertEventsdb = insertEventsMongo.database(eventsdbConfig.getString("database", "events-v" + serviceVersion))
          val queryEventsMongo = mongoFactory(eventsdbConfig)
          val queryEventsdb = queryEventsMongo.database(eventsdbConfig.getString("database", "events-v" + serviceVersion))
          val indexdbConfig = config.configMap("indexdb")
          val indexMongo = mongoFactory(indexdbConfig)
          val indexdb  = indexMongo.database(indexdbConfig.getString("database", "analytics-v" + serviceVersion))

          val tokensCollection = config.getString("tokens.collection", "tokens")
          val deletedTokensCollection = config.getString("tokens.deleted", "deleted_tokens")
          val tokenMgr = tokenManager(indexdb, tokensCollection, deletedTokensCollection)

          for (aggregationEngine <- AggregationEngine(config, logger, insertEventsdb, queryEventsdb, indexdb, monitor)) yield {
            AnalyticsState(
              insertEventsMongo,
              queryEventsMongo,
              indexMongo,
              aggregationEngine, 
              tokenMgr, 
              storageReporting(config.configMap("storageReporting")),
              auditClient(config.configMap("audit")),
              jessup(config.configMap("jessup")),
              config.getInt("raw_event_fetch_limit", 1000)
            )
          }
        } ->
        request { (state: AnalyticsState) =>
          import state.{aggregationEngine}

          val audit = auditor(state.auditClient, clock, state.tokenManager)
          import audit._

                 
          import FormatServiceHelpers.{logger => _, _}
          val jsonpFormatter = jsonToJsonp[ByteChunk]
                 
          val formatters = Map("jsonp" -> jsonpFormatter,
                               "csv"   -> jsonToCSV[ByteChunk])

          (new SaveQueryService(state.queryEventsMongo.database("querycache1"))) {
          formatService[ByteChunk](formatters, jsonpFormatter) {
            token(state.tokenManager) {
              /* The virtual file system, which is used for storing data,
               * retrieving data, and querying for metadata.
               */
              path("/store") {
                dataPath("vfs") {
                  post(new TrackingService(aggregationEngine, state.storageReporting, timeSeriesEncoding, clock, state.jessup, false)).audited("store")
                }
              } ~ 
              dataPath("vfs") {
                post(new TrackingService(aggregationEngine, state.storageReporting, timeSeriesEncoding, clock, state.jessup, true)).audited("track") ~
                get(new ExplorePathService[Future[JValue]](aggregationEngine)).audited("explore paths") ~
                variable {
                  path(/?) {
                    get {
                      new ExploreVariableService[Future[JValue]](aggregationEngine).audited("explore variable children") 
                    }
                  } ~
                  path("/") { 
                    commit {
                      path("events") {
                        new RawEventService(aggregationEngine, state.defaultRawLimit).audited("Retrieve raw events")
                      } ~
                      path("count") {
                        new VariableCountService(aggregationEngine).audited("count occurrences of a variable")
                      } ~ 
                      path("properties/") {
                        path("count") {
                          new VariableChildCountService(aggregationEngine).audited("count children of a variable")
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
                              path("sums") {
                                new VariableSeriesService(aggregationEngine, _.sum).audited("variable sum series")
                              } ~ 
                              path("means") {
                                new VariableSeriesService(aggregationEngine, _.mean).audited("variable mean series")
                              } ~ 
                              path("standardDeviations") {
                                new VariableSeriesService(aggregationEngine, _.standardDeviation).audited("variable standard deviation series")
                              } 
                            }
                          }
                        }
                      } ~ 
                      path("histogram") {
                        path(/?) {
                          get { 
                            audited("Return a histogram of values of a variable.") { 
                              (r: HttpRequest[Future[JValue]]) => HistogramService.getHistogram(r, aggregationEngine, _: Token, _: Path, _: Variable, 0)
                            }
                          }
                        } ~
                        commit {
                          path("/") {
                            pathData("top/'limit", 'limit, parsePathInt("limit")) {
                              get { 
                                audited("Return a histogram of the top 'limit values of a variable, by count.") { 
                                  (r: HttpRequest[Future[JValue]]) => 
                                    (limit: Int) => HistogramService.getHistogram(r, aggregationEngine, _: Token, _: Path, _: Variable, limit)
                                }
                              }
                            } ~
                            pathData("bottom/'limit", 'limit, parsePathInt("limit")) {
                              get { 
                                audited("Return a histogram of the bottom 'limit values of a variable, by count.") { 
                                  (r: HttpRequest[Future[JValue]]) => 
                                    (limit: Int) => HistogramService.getHistogram(r, aggregationEngine, _: Token, _: Path, _: Variable, -limit)
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
                            audited("list of variable values") {
                              (req: HttpRequest[Future[JValue]]) =>
                                new ExploreValuesService(aggregationEngine,0).service(req)
                            }
                          }
                        } ~
                        path("/") {
                          commit {
                            pathData("top/'limit", 'limit, parsePathInt("limit")) {
                              get { 
                                audited("List the top n variable values") {
                                  (req: HttpRequest[Future[JValue]]) => 
                                    (limit: Int) => new ExploreValuesService(aggregationEngine, limit).service(req)
                                }
                              }
                            } ~
                            pathData("bottom/'limit", 'limit, parsePathInt("limit")) {
                              get { 
                                audited("list of bottom variable values") {
                                  (req: HttpRequest[Future[JValue]]) => 
                                    (limit: Int) => new ExploreValuesService(aggregationEngine, -limit).service(req)
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
                                      new ValueCountService(aggregationEngine).audited("count occurrences of a variable value") 
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
              dataPath("tags") {
                get(new ExploreTagsService[Future[JValue]](aggregationEngine).audited("Explore tags for a path")) ~
                variable {
                  path(/?) {
                    get {
                      new ExploreHierarchyService[Future[JValue]](aggregationEngine).audited("explore variable children") 
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
                  TokenService(state.tokenManager, clock, logger)
                }
              }
            }
          }
          }
        } ->
        shutdown { state => 
          
          // So why st and impStop[T] you might ask?
          // Well the implicit timeout used for healthMonitor, requestLogging, etc above needs to be short to
          // prevent akka timeout related memory leaks

          // implicit timeout already in scope means we need to specify explicitly
          // added impStop to make implicit stop expression simpler
          // not greate but trying to get this fix into production and get back to v2

          def impStop[T](implicit s: Stop[T]): Stop[T] = s 
          val st = akka.actor.Actor.Timeout(Long.MaxValue)

          Future.sync( 
            Option(
              Stoppable(
                state.aggregationEngine, 
                Stoppable(state.aggregationEngine.queryIndexdb, Stoppable(state.indexMongo)(impStop[Mongo], st) :: Nil)(impStop[Database], st) ::
                Stoppable(state.aggregationEngine.insertEventsdb, Stoppable(state.insertEventsMongo)(impStop[Mongo], st) :: Nil)(impStop[Database], st) ::
                Stoppable(state.aggregationEngine.queryEventsdb, Stoppable(state.queryEventsMongo)(impStop[Mongo], st) :: Nil)(impStop[Database], st) :: Nil
              )(impStop[AggregationEngine], st)
            )
          )
        }
      }
    }
  }}

}

object AnalyticsService extends HttpRequestHandlerCombinators with PartialFunctionCombinators with Logging {
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
      case field                        => field
    }
  )
    
  def shiftTimeSeries[V: AbelianGroup : MDouble](periodicity: Periodicity, zone: Option[DateTimeZone]): Endo[ResultSet[JObject, V]] = {
    import SAct._

    (resultSet: ResultSet[JObject, V]) => {
      val shifted : ResultSet[JObject, V] = zone filter (_ != DateTimeZone.UTC) map { zone => 
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
      
      if (shifted.isEmpty) shifted else shifted.tail.filterNot { case (key: JObject, _) => (key \ "bogus") == JBool(true) } //since intervalTerm extends the query interval back in time, but remove bogus entry
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
            val key = normalize(JObject(
              obj.fields.flatMap {
                case JField("datetime",  datetime) => newField(datetime.deserialize[DateTime](ZonedTimeExtractor(zone.getOrElse(DateTimeZone.UTC))))
                case JField("timestamp", instant)  => newField(instant.deserialize[Instant].toDateTime(DateTimeZone.UTC))
                case field => Some(field)
              }))

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
    val offset = parameters.get('tzoffset).flatMap { 
      case "" => logger.warn("Empty tzoffset sent"); None
      case o  => try { Some(new Duration(o.toLong)) } catch { case e => logger.warn("Error parsing tzoffset:" + e.getMessage); None }
    } getOrElse(Duration.ZERO)

    logger.trace("Interval offset = " + offset)
    validated(timeSpan(parameters, content)).map(IntervalTerm(timeSeriesEncoding, periodicity, _, offset).extendForInterpolation)
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
