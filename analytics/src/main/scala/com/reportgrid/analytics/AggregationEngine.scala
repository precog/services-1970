package com.reportgrid.analytics

import blueeyes._
import blueeyes.concurrent._
import blueeyes.persistence.mongo._
import blueeyes.persistence.cache.{Stage, ExpirationPolicy, CacheSettings}
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._

import com.reportgrid.analytics._
import com.reportgrid.analytics.AggregatorImplicits._
import com.reportgrid.analytics.persistence.MongoSupport._
import com.reportgrid.ct._

import java.util.concurrent.TimeUnit

import net.lag.configgy.ConfigMap
import net.lag.logging.Logger

import org.joda.time.Instant

import scala.collection.SortedMap
import scalaz.{Ordering => _, _}
import Scalaz._
import Future._
import Hashable._
import SignatureGen._

/**
 * Schema:
 * 
 * variable_value_series: {
 *   _id: mongo-generated,
 *   id: hashSig(accountTokenId, path, variable, order, observation, period, granularity),
 *   counts: { "0123345555555": 3, ...}
 * }
 *
 * variable_series: {
 *   _id: mongo-generated,
 *   id: hashSig(accountTokenId, path, variable, order, period, granularity),
 *   counts: { "0123345555555": {"count": 3, "type": "Int", "sum": 123, "sumsq": 999}, ...}
 * }
 *
 * variable_values: {
 *   _id: mongo-generated,
 *   id: hashSig(accountTokenId, path, variable),
 *   values: {
 *     "male" : 23,
 *     "female" : 42,
 *     ...
 *   }
 * }
 *
 * variable_values_infinite: {
 *   _id: mongo-generated,
 *   id: hashSig(tokenId, path, variable),
 *   value: "John Doe"
 *   count: 42
 * }
 * 
 * path_children: {
 *   _id: mongo-generated,
 *   accountTokenId: "foobar"
 *   path: "baz/blork",
 *   child: "blaarg",
 *   count: 1
 * }
 *
 * variable_children: {
 *   _id: mongo-generated,
 *   accountTokenId: "foobar"
 *   path: "/baz/blork/blaarg/.gweep"
 *   child: "toodleoo",
 *   count: 1
 * }
 */

case class AggregationStage(collection: MongoCollection, stage: MongoStage) {
  def put(filter: MongoFilter, update: MongoUpdate): Unit = stage.put(filter & collection, update)
  def put(t: (MongoFilter, MongoUpdate)): Unit = this.put(t._1, t._2)
  def putAll(filters: Iterable[(MongoFilter, MongoUpdate)]): Unit = {
    stage.putAll(filters.map(((_: MongoFilter).&(collection)).first[MongoUpdate]))
  }
}

class AggregationEngine private (config: ConfigMap, logger: Logger, database: Database) {
  import AggregationEngine._

  val EarliestTime = new Instant(0)
  val LatestTime   = new Instant(Long.MaxValue)

  val ChildReportEmpty    = Report.empty[HasChild, CountType]
  val ValueReportEmpty    = Report.empty[HasValue, TimeSeries[CountType]]

  val timeSeriesEncoding  = TimeSeriesEncoding.Default
  val timeGranularity     = timeSeriesEncoding.grouping.keys.min
  implicit val HashSig    = Sha1HashFunction

  private def AggregationStage(prefix: String): AggregationStage = {
    val timeToIdle      = config.getLong(prefix + ".time_to_idle_millis").getOrElse(10000L)
    val timeToLive      = config.getLong(prefix + ".time_to_live_millis").getOrElse(10000L)
    val initialCapacity = config.getInt (prefix + ".initial_capacity").getOrElse(1000)
    val maximumCapacity = config.getInt (prefix + ".maximum_capacity").getOrElse(10000)

    val collection = config.getString(prefix + ".collection").getOrElse(prefix)

    new AggregationStage(
      collection = collection,
      stage = new MongoStage(
        database   = database,
        mongoStageSettings = MongoStageSettings(
          expirationPolicy = ExpirationPolicy(
            timeToIdle = Some(timeToIdle),
            timeToLive = Some(timeToLive),
            unit       = TimeUnit.MILLISECONDS
          ),
          maximumCapacity = maximumCapacity
        )
      )
    )
  }

  private val variable_series           = AggregationStage("variable_series")
  private val variable_value_series     = AggregationStage("variable_value_series")

  private val variable_values           = AggregationStage("variable_values")
  private val variable_values_infinite  = AggregationStage("variable_values_infinite")
  private val variable_children         = AggregationStage("variable_children")
  private val path_children             = AggregationStage("path_children")


  /** Aggregates the specified data. The object may contain multiple events or
   * just one.
   */
  def aggregate(token: Token, path: Path, time: Instant, jobject: JObject, count: Long) = Future.async {
    // Keep track of parent/child relationships:
    path_children putAll addPathChildrenOfPath(token, path).patches

    val seriesCount = TimeSeries.point(timeGranularity, time, count)

    jobject.fields.foreach {
      case field @ JField(eventName, _) => 
        val event = JObject(field :: Nil)

        path_children put addChildOfPath(forTokenAndPath(token, path), "." + eventName)

        val (finiteReport, infiniteReport) = Report.ofValues(
          event = event,
          count = seriesCount,
          order = token.limits.order,
          depth = token.limits.depth,
          limit = token.limits.limit
        )

        val finiteOrder1 = finiteReport.order(1)

        val (vvSeriesPatches, vvInfinitePatches) = updateTimeSeries(token, path, finiteReport, infiniteReport)
        variable_value_series putAll vvSeriesPatches.patches

        variable_values          putAll updateFiniteValues(token, path, finiteOrder1, count).patches
        variable_values_infinite putAll vvInfinitePatches.patches

        val childCountReport = Report.ofChildren(
          event = event,
          count = count,
          order = 1,
          depth = token.limits.depth,
          limit = token.limits.limit
        )

        variable_children putAll updateChildren(token, path, childCountReport).patches

        val childSeriesReport = Report.ofInnerNodes(
          event = event,
          count = seriesCount,
          order = 1,
          depth = token.limits.depth,
          limit = token.limits.limit
        )

        variable_series putAll updateVariableSeries(token, path, childSeriesReport, finiteOrder1 + infiniteReport).patches
    }
  }

  /** Retrieves children of the specified path &amp; variable.  */
  def getVariableChildren(token: Token, path: Path, variable: Variable): Future[List[HasChild]] = {
    extractValues(forTokenAndPath(token, path) & forVariable(variable), variable_children.collection) { (jvalue, _) =>
      jvalue.deserialize[HasChild]
    }
  }

  /** Retrieves children of the specified path.  */
  def getPathChildren(token: Token, path: Path): Future[List[String]] = {
    extractChildren(forTokenAndPath(token, path), path_children.collection) { (jvalue, _) =>
      jvalue.deserialize[String]
    }
  }
  
  def getHistogram(token: Token, path: Path, variable: Variable): Future[Map[HasValue, CountType]] = 
    getHistogramInternal(token, path, variable)

  def getHistogramTop(token: Token, path: Path, variable: Variable, n: Int): Future[List[(HasValue, CountType)]] = 
    getHistogramInternal(token, path, variable).map(_.toList.sortBy(- _._2).take(n))

  def getHistogramBottom(token: Token, path: Path, variable: Variable, n: Int): Future[List[(HasValue, CountType)]] = 
    getHistogramInternal(token, path, variable).map(_.toList.sortBy(_._2).take(n))

  /** Retrieves values of the specified variable.
   */
  def getValues(token: Token, path: Path, variable: Variable): Future[Iterable[HasValue]] = 
    getHistogramInternal(token, path, variable).map(_.map(_._1))

  def getValuesTop(token: Token, path: Path, variable: Variable, n: Int): Future[List[HasValue]] = 
    getHistogramTop(token, path, variable, n).map(_.map(_._1))

  def getValuesBottom(token: Token, path: Path, variable: Variable, n: Int): Future[List[HasValue]] = 
    getHistogramBottom(token, path, variable, n).map(_.map(_._1))

  /** Retrieves the length of array properties, or 0 if the property is not an array.
   */
  def getVariableLength(token: Token, path: Path, variable: Variable): Future[Int] = {
    getVariableChildren(token, path, variable).map { hasChildren =>
      hasChildren.map(_.child.toString).filterNot(_.endsWith("/")).map(JPath(_)).foldLeft(0) {
        case (length, jpath) =>
          jpath.nodes match {
            case JPathIndex(index) :: Nil => (index + 1).max(length)
            case _ => length
          }
      }
    }
  }

  def getVariableStatistics(token: Token, path: Path, variable: Variable): Future[Statistics] = {
    getHistogram(token, path, variable).map { histogram =>
      (histogram.foldLeft(RunningStats.zero) {
        case (running, (hasValue, count)) =>
          val number = hasValue.value.deserialize[Double]

          running.update(number, count)
      }).statistics
    }
  }

  /** Retrieves a count of how many times the specified variable appeared in a path */
  def getVariableCount(token: Token, path: Path, variable: Variable): Future[CountType] = {
    getVariableSeries(token, path, variable, Periodicity.Eternity).map(_.total.count)
  }

  /** Retrieves a time series of statistics of occurrences of the specified variable in a path */
  def getVariableSeries(token: Token, path: Path, variable: Variable, 
                        granularity: Periodicity, start : Option[Instant] = None, end : Option[Instant] = None): 
                        Future[TimeSeries[ValueStats]] = {

    internalSearchSeries[ValueStats](token, path, granularity, start, end, variableSeriesKey(token, path, variable, _: Period, granularity), "data", variable_series.collection)
  }

  /** Retrieves a count of the specified observed state over the given time period */
  def searchCount(token: Token, path: Path, observation: Observation[HasValue],
                  start : Option[Instant] = None, end : Option[Instant] = None): Future[CountType] = {

    searchPeriod(start, end, searchSeries(token, path, observation, _, _, _)) map {
      _.map(_.total).asMA.sum
    }
  }

  /** Retrieves a time series of counts of the specified observed state
   *  over the given time period.
   */
  def searchSeries(token: Token, path: Path, observation: Observation[HasValue], 
                   granularity: Periodicity, start : Option[Instant] = None, end : Option[Instant] = None): 
                   Future[TimeSeries[CountType]] = {

    internalSearchSeries[CountType](token, path, granularity, start, end, valueSeriesKey(token, path, observation, _: Period, granularity), "counts", variable_value_series.collection)
  }

  def intersectCount(token: Token, path: Path, properties: List[VariableDescriptor],
                     start: Option[Instant] = None, end: Option[Instant] = None): Future[IntersectionResult[CountType]] = {

    searchPeriod(start, end, intersectSeries(token, path, properties, _, _, _)) map {
      _.foldLeft(SortedMap.empty[List[JValue], CountType](ListJValueOrdering)) {
        case (total, partialResult) => partialResult.foldLeft(total) {
          case (total, (key, timeSeries)) => 
            total + (key -> total.get(key).map(_ |+| timeSeries.total).getOrElse(timeSeries.total))
        }
      } 
    }
  }

  def intersectSeries(token: Token, path: Path, properties: List[VariableDescriptor], 
                      granularity: Periodicity, start: Option[Instant] = None, end: Option[Instant] = None): 
                      Future[IntersectionResult[TimeSeriesType]] = {

    intersectValueSeries(token, path, properties, granularity, start, end) 
  }

  private def searchPeriod[T](start: Option[Instant], end: Option[Instant], f: (Periodicity, Option[Instant], Option[Instant]) => Future[T]): Future[List[T]] = {
    Future {
      (start <**> end) {
        (start, end) => timeSeriesEncoding.queriableExpansion(start, end) map {
          case (p, s, e) => f(p, Some(s), Some(e))
        }
      } getOrElse {
        f(Periodicity.Eternity, None, None) :: Nil
      }.toSeq: _*
    }
  }

  private def internalSearchSeries[T: AbelianGroup : Extractor](token: Token, path: Path, granularity: Periodicity, start : Option[Instant], end : Option[Instant],
                                                        f: Period => MongoFilter, dataPath: String, collection: MongoCollection): Future[TimeSeries[T]] = {

    val batchPeriodicity = timeSeriesEncoding.grouping(granularity)
    val interval = Interval(start, end, granularity)
    val intervalFilters = interval.mapBatchPeriods(batchPeriodicity)(f)

    Future {
      intervalFilters.map(filter => database(selectOne("." + dataPath).from(collection).where(filter))).toSeq: _*
    } map {
      _.flatten
       .foldLeft(TimeSeries.empty[T](granularity)) {
         (series, result) => ((result \ dataPath) -->? classOf[JObject]) map { jobj => 
           series + interval.deserializeTimeSeries[T](jobj)
         } getOrElse {
           series
         }
       }
       .fillGaps(start, end)
    }
  }

  private def intersectValueSeries(
      token: Token, path: Path, variableDescriptors: List[VariableDescriptor], 
      granularity: Periodicity, start : Option[Instant], end : Option[Instant]): 
      Future[IntersectionResult[TimeSeriesType]] = { 

    val variables = variableDescriptors.map(_.variable)

    val futureHistograms: Future[List[Map[HasValue, CountType]]]  = Future {
      variableDescriptors.map { 
        case VariableDescriptor(variable, maxResults, SortOrder.Ascending) =>
          getHistogramBottom(token, path, variable, maxResults).map(_.toMap)

        case VariableDescriptor(variable, maxResults, SortOrder.Descending) =>
          getHistogramTop(token, path, variable, maxResults).map(_.toMap)
      }: _*
    }

    futureHistograms.flatMap { histograms  => 
      implicit val resultOrder = intersectionOrder(variableDescriptors.map(_.sortOrder) zip histograms)

      def observations[P <: Predicate](vs: List[Variable]): Iterable[Observation[HasValue]] = {
        def obs(i: Int, v: Variable, vs: List[Variable], o: Observation[HasValue]): Iterable[Observation[HasValue]] = {
          histograms(i).flatMap { 
            case (hasValue, _) => vs match {
              case Nil => List(o + ((v, hasValue)))
              case vv :: vvs => obs(i + 1, vv, vvs, o + ((v, hasValue)))
            }
          }
        }

        vs match {
          case Nil => Nil
          case v :: vs => obs(0, v, vs, Obs.empty[HasValue])
        }
      }

      val variables = variableDescriptors.map(_.variable)
      Future {
        observations(variables).map { obs => 
          val obsMap = obs.toMap
          searchSeries(token, path, obs, granularity, start, end).map { result => 
            (variables.map(obsMap).map(_.value).toList -> result)
          }
        }.toSeq: _*
      } map {
        _.foldLeft(SortedMap.empty[List[JValue], TimeSeriesType]) {
          case (results, (k, v)) => 
            results + (k -> results.get(k).map(_ |+| v).getOrElse(v))
        }
      }
    }
  }

  private def valuesKeyFilter(token: Token, path: Path, variable: Variable) = {
    JPath("." + valuesId) === hashSignature(token.sig ++ path.sig ++ variable.sig)
  }

  /** Retrieves a histogram of the values a variable acquires over its lifetime.
   */
  private def getHistogramInternal(token: Token, path: Path, variable: Variable): Future[Map[HasValue, CountType]] = {
    type R = (HasValue, CountType)
    getVariableLength(token, path, variable).flatMap { 
      case 0 =>
        val extractor = if (variable.name.endsInInfiniteValueSpace) {
          extractInfiniteValues[R](valuesKeyFilter(token, path, variable), variable_values_infinite.collection) _
        } else {
          extractValues[R](valuesKeyFilter(token, path, variable), variable_values.collection) _
        }

        extractor((jvalue, count) => (jvalue.deserialize[HasValue], count)) map (_.toMap)

      case length =>
        Future((0 until length).map { index =>
          getHistogramInternal(token, path, Variable(variable.name \ JPathIndex(index)))
        }: _*).map { results =>
          results.foldLeft(Map.empty[HasValue, CountType]) {
            case (all, cur) => all <+> cur 
          }
        }
    }    
  }

  private def infiniteSeriesKey[P <: Predicate : SignatureGen](token: Token, path: Path, observation: Observation[P]) = {
    JPath("." + seriesId) === hashSignature(token.sig ++ path.sig ++ observation.sig)
  }

  private def valueSeriesKey[P <: Predicate : SignatureGen](token: Token, path: Path, observation: Observation[P],
                                                             period: Period, granularity: Periodicity): MongoFieldFilter = {
    JPath("." + seriesId) === hashSignature(
      token.sig ++ path.sig ++ 
      period.sig ++ granularity.sig ++
      observation.sig
    )
  }

  private def variableSeriesKey(token: Token, path: Path, variable: Variable, period: Period, granularity: Periodicity) = {
    JPath("." + seriesId) === hashSignature(
      token.sig ++ path.sig ++ variable.sig ++ 
      period.sig ++ granularity.sig 
    )
  }

  private def extractChildren[T](filter: MongoFilter, collection: MongoCollection)(extractor: (JValue, CountType) => T): Future[List[T]] = {
    database {
      select(".child", ".count").from(collection).where(filter)
    } map {
      _.foldLeft(List.empty[T]) { 
        case (l, result) => 
          val child = (result \ "child")
          val count = (result \ "count").deserialize[CountType]
          extractor(child, count) :: l
      }
    }
  }

  private def extractValues[T](filter: MongoFilter, collection: MongoCollection)(extractor: (JValue, CountType) => T): Future[List[T]] = {
    database {
      selectOne(".values").from(collection).where(filter)
    } map { 
      case None => Nil

      case Some(result) =>
        (result \ "values").children.collect {
          case JField(name, count) =>
            val jvalue = JsonParser.parse(MongoEscaper.decode(name))

            extractor(jvalue, count.deserialize[CountType])
        }
    }
  }

  private def extractInfiniteValues[T](filter: MongoFilter, collection: MongoCollection)(extractor: (JValue, CountType) => T): Future[Iterable[T]] = {
    database {
      select(".value", ".count").from(collection).where(filter)
    } map { 
      _.map(result => extractor((result \ "value"), (result \ "count").deserialize[CountType]))
    }
  }

  private def forTokenAndPath(token: Token, path: Path): MongoFilter = {
    (".accountTokenId" === token.accountTokenId) &
    (".path"           === path.toString)
  }

  private def forVariable(variable: Variable): MongoFilter = {
    ".variable" === variable.serialize
  }

  /*********************
   * UPDATE GENERATION *
   *********************/

  /** Creates a bunch of patches to keep track of parent/child path relationships.
   * E.g. if you send "/foo/bar/baz", it will keep track of the following:
   *
   * "/foo" has child "bar"
   * "/foo/bar" has child "baz"
   */
  private def addPathChildrenOfPath(token: Token, path: Path): MongoPatches = {
    val patches = path.parentChildRelations.foldLeft(MongoPatches.empty) { 
      case (patches, (parent, child)) =>
        patches + addChildOfPath(forTokenAndPath(token, parent), child.elements.last)
    }

    patches
  }

  /** Pushes the specified name onto a "." member of a document. This
   * function is used to keep track of the layout of the virtual file system.
   */
  private def addChildOfPath(filter: MongoFilter, child: String): (MongoFilter, MongoUpdate) = {
    ((filter & (".child" === child.serialize)) -> (".count" inc 1))
  }

  /** Creates patches to record variable observations.
   */
  private def updateFiniteValues[P <: Predicate : Decomposer, T](token: Token, path: Path, report: Report[P, T], count: CountType): MongoPatches = {
    report.observationCounts.foldLeft(MongoPatches.empty) { 
      case (patches, (observation, _)) => observation.foldLeft(patches) {
        case (patches, (variable, predicate)) =>

          val predicateField = MongoEscaper.encode(renderNormalized(predicate.serialize))
          val valuesUpdate = (JPath(".values") \ JPathField(predicateField)) inc count

          patches + (valuesKeyFilter(token, path, variable) -> valuesUpdate)
      }
    }
  }

  /** Creates patches to record variable observations.
   */
  private def updateChildren[P <: Predicate : Decomposer, T](token: Token, path: Path, report: Report[P, CountType]): MongoPatches = {
    report.observationCounts.foldLeft(MongoPatches.empty) { 
      case (patches, (observation, count)) => observation.foldLeft(patches) {
        case (patches, (variable, predicate)) =>

          val filterVariable = forTokenAndPath(token, path) & forVariable(variable)
          val predicateField = MongoEscaper.encode(renderNormalized(predicate.serialize))

          val valuesUpdate = (JPath(".values") \ JPathField(predicateField)) inc count

          patches + (filterVariable -> valuesUpdate)
      }
    }
  }

  private def buildSeriesAggregates[P <: Predicate](report: Report[P, TimeSeries[CountType]]) = {
    // aggregate time series up the scale of coarser granularities.   
    report.observationCounts.mapValues(_.aggregates).
    // build a map from period, contained granularity, and observation to time series at that granularity
    foldLeft(Map.empty[(Period, Periodicity, Observation[P]), TimeSeries[CountType]]) {
      case (m, (observation, multiSeries)) => multiSeries.foldLeft(m) {
        case (m, TimeSeries(granularity, counts)) => 
          val tsZero = TimeSeries.empty[CountType](granularity)
          counts.foldLeft(m) {
            case (m, entry @ (start, _)) =>
              val batchKey = (timeSeriesEncoding.grouping(granularity).period(start), granularity, observation)
              m + (batchKey -> (m.getOrElse(batchKey, tsZero) + entry))
          }
      }
    }  
  }

  private def updateTimeSeries(token: Token, path: Path, finiteReport: Report[HasValue, TimeSeries[CountType]], infiniteReport: Report[HasValue, TimeSeries[CountType]]): (MongoPatches, MongoPatches) = {
    buildSeriesAggregates(finiteReport).foldLeft((MongoPatches.empty, MongoPatches.empty)) {
      case ((finitePatches, infinitePatches), ((period, _, observation), TimeSeries(granularity, counts))) => 
        val finiteKey = valueSeriesKey(token, path, observation, period, granularity)
        val mongoUpdate = counts.foldLeft[MongoUpdate](MongoUpdateNothing) {
          case (fullUpdate, (time, count)) =>
            fullUpdate |+| ((JPath(".counts") \ time.getMillis.toString) inc count)
        }

        Tuple2(
          finitePatches + (finiteKey -> mongoUpdate),
          //build infinite patches by adding the finite key id to the list of locations that the infinite value
          //was observed in conjunction with.
          infinitePatches ++ infiniteReport.observationCounts.foldLeft(MongoPatches.empty) {
            case (patches, (observation, pointCounts)) => observation.foldLeft(patches) { 
              //should only be one element in this set; this is the contract of the infinite component of Report.ofValues
              case (patches, (variable, value)) =>
                val infiniteKey = infiniteSeriesKey(token, path, observation) 
                // The infinite counts timeseries should have a single entry, because it was derived from TimeSeries.point.
                val infiniteCount = pointCounts.series.head.fold((_, count) => (JPath(".count") inc count))
                patches + (
                  infiniteKey -> (
                    (".ids" push finiteKey.rhs) |+| 
                    (MongoUpdateBuilder(".variable") set variable.serialize) |+| 
                    (MongoUpdateBuilder(".value") set value.serialize) |+| 
                    infiniteCount
                  )
                )
            }
          }
        )
    }
  }

  private def updateVariableSeries(token: Token, path: Path, 
                                   children: Report[HasChild, TimeSeries[CountType]],
                                   values:   Report[HasValue, TimeSeries[CountType]]): MongoPatches = {

    def countUpdate(time: Instant, count: CountType): MongoUpdate = {
        JPath("data", time.getMillis.toString, "count") inc count
    }

    def buildUpdate(variable: Variable, jvalue: JValue, time: Instant, count: CountType): MongoUpdate = {
      import scala.math.BigInt._

      // build a patch including the count, sum, and sum of the squares of integral values
      val updates = List(
        Some(countUpdate(time, count)),
        jvalue option {
          case JInt(i)    => JPath("data", time.getMillis.toString, "sum") inc (count * i)
          case JDouble(i) => JPath("data", time.getMillis.toString, "sum") inc (count * i)
          case JBool(i)   => JPath("data", time.getMillis.toString, "sum") inc (count * (if (i) 1 else 0))
        },
        jvalue option {
          case JInt(i)    => JPath("data", time.getMillis.toString, "sumsq") inc (count * (i * i))
          case JDouble(i) => JPath("data", time.getMillis.toString, "sumsq") inc (count * (i * i))
        }, 
        jvalue option {
          case JString(s) if !variable.name.endsInInfiniteValueSpace => 
            JPath("data", time.getMillis.toString, "values", MongoEscaper.encode(s)) inc count
        }
      )
      
      updates.flatten.foldLeft[MongoUpdate](MongoUpdateNothing)(_ |+| _)
    }

    def buildPatches[P <: Predicate](report: Report[P, TimeSeries[CountType]]) = {
      buildSeriesAggregates(report).foldLeft(MongoPatches.empty) {
        case (patches, ((period, _, observation), TimeSeries(granularity, counts))) => counts.foldLeft(patches) {
          case (patches, (time, count)) => observation.foldLeft(patches) {
            case (patches, (Variable(vpath), HasChild(child))) => 
              // this resuse of valueSeriesKey seems a little problematic
              val key = variableSeriesKey(token, path, Variable(vpath \ child), period, granularity)
              patches + (key -> countUpdate(time, count))

            case (patches, (variable, HasValue(jvalue))) =>             
              val key = variableSeriesKey(token, path, variable, period, granularity)
              patches + (key -> buildUpdate(variable, jvalue, time, count))
          }
        }
      }
    }

    buildPatches(children) ++ buildPatches(values)
  }

  def stop(): Future[Unit] =  for {
    _ <- variable_value_series.stage.flushAll
    _ <- variable_series.stage.flushAll
    _ <- variable_children.stage.flushAll
    _ <- variable_values.stage.flushAll
    _ <- path_children.stage.flushAll
  } yield ()
}

object AggregationEngine {
  type CountType             = Long
  type TimeSeriesType        = TimeSeries[CountType]
  type IntersectionResult[T] = SortedMap[List[JValue], T]

  type ChildReport        = Report[HasChild, CountType]
  type ValueReport        = Report[HasValue, TimeSeries[CountType]]

  private val seriesId = "id"
  private val valuesId = "id"

  private val CollectionIndices = Map(
    "variable_series" -> Map(
      "var_series_id" -> (List(seriesId), true)
    ),
    "variable_value_series" -> Map(
      "var_val_series_id" -> (List(seriesId), true)
    ),
    "variable_values" -> Map(
      "var_val_id" -> (List(valuesId), true)
    ),
    "variable_values_infinite" -> Map(
      "var_val_id" -> (List(valuesId, "value"), true)
    ),
    "variable_children" -> Map(
      "variable_query" -> (List("path", "accountTokenId", "variable"), false)
    ),
    "path_children" -> Map(
      "path_query" -> (List("path", "accountTokenId"), false),
      "path_child_query" -> (List("path", "accountTokenId", "child"), false)
    )
  )

  private def createIndices(database: Database) = {
    val futures = for ((collection, indices) <- CollectionIndices; 
                       (indexName, (fields, unique)) <- indices) yield {
      database[JNothing.type] {
        if (unique) ensureUniqueIndex(indexName + "_index").on(fields.map(JPath(_)): _*).in(collection)
        else ensureIndex(indexName + "_index").on(fields.map(JPath(_)): _*).in(collection)
      }.toUnit
    }

    Future(futures.toSeq: _*)
  }

  val ListJValueOrdering: Ordering[List[JValue]] = new Ordering[List[JValue]] {
    import blueeyes.json.xschema.DefaultOrderings.JValueOrdering

    def compare(l1: List[JValue], l2: List[JValue]): Int = {
      (l1.zip(l2).map {
        case (v1, v2) => JValueOrdering.compare(v1, v2)
      }).dropWhile(_ == 0).headOption match {
        case None => l1.length compare l2.length
        
        case Some(c) => c
      }
    }
  }

  def apply(config: ConfigMap, logger: Logger, database: Database): Future[AggregationEngine] = {
    createIndices(database).map(_ => new AggregationEngine(config, logger, database))
  }

  def intersectionOrder[T <% Ordered[T]](histograms: List[(SortOrder, Map[HasValue, T])]): scala.math.Ordering[List[JValue]] = {
    new scala.math.Ordering[List[JValue]] {
      override def compare(l1: List[JValue], l2: List[JValue]) = {
        val valueOrder = (l1 zip l2).zipWithIndex.foldLeft(0) {
          case (0, ((v1, v2), i)) => 
            val (sortOrder, m) = histograms(i)  
            sortOrder match {
              case SortOrder.Ascending  => -(m(HasValue(v1)) compare m(HasValue(v2)))
              case SortOrder.Descending =>   m(HasValue(v1)) compare m(HasValue(v2))
            }
           
          case (x, _) => x
        }

        if (valueOrder == 0) ListJValueOrdering.compare(l1, l2) else valueOrder
      }
    }   
  }
}
