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

import net.lag.configgy.ConfigMap
import net.lag.logging.Logger

import org.joda.time.{DateTime, DateTimeZone}

import java.util.concurrent.TimeUnit

import com.reportgrid.analytics.AggregatorImplicits._
import com.reportgrid.analytics.persistence.MongoSupport._
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
 *   _id: hashSig(accountTokenId, path, variable, order, observation, period, granularity),
 *   counts: { "0123345555555": 3, ...}
 * }
 *
 * variable_series: {
 *   _id: hashSig(accountTokenId, path, variable, order, period, granularity),
 *   counts: { "0123345555555": 3, ...}
 * }
 *
 * variable_values: {
 *   _id: hashSig(accountTokenId, path, variable),
 *   values: {
 *     "male" : 20,
 *     "female" : 30,
 *     ...
 *   }
 * }
 *
 * path_children: {
 *   _id: mongo-generated,
 *   accountTokenId: "foobar"
 *   path: "baz/blork",
 *   child: "blaarg"
 * }
 *
 * variable_children: {
 *   _id: mongo-generated,
 *   accountTokenId: "foobar"
 *   path: "/baz/blork/blaarg/.gweep"
 *   child: "toodleoo"
 * }
 */

class AggregationEngine private (config: ConfigMap, logger: Logger, database: MongoDatabase) {
  import AggregationEngine._

  val EarliestTime = new DateTime(0,             DateTimeZone.UTC)
  val LatestTime   = new DateTime(Long.MaxValue, DateTimeZone.UTC)

  type CountType          = Long
  type TimeSeriesType   = TimeSeries[CountType]

  type ChildReport        = Report[HasChild, CountType]
  val  ChildReportEmpty   = Report.empty[HasChild, CountType]

  type ValueReport        = Report[HasValue, TimeSeries[CountType]]
  val  ValueReportEmpty   = Report.empty[HasValue, TimeSeries[CountType]]

  val timeSeriesEncoding  = TimeSeriesEncoding.Default
  val timeGranularity     = timeSeriesEncoding.grouping.keys.min
  implicit val HashSig    = Sha1HashFunction

  private def newMongoStage(prefix: String): MongoStage = {
    val timeToIdle      = config.getLong(prefix + ".time_to_idle_millis").getOrElse(10000L)
    val timeToLive      = config.getLong(prefix + ".time_to_live_millis").getOrElse(10000L)
    val initialCapacity = config.getInt (prefix + ".initial_capacity").getOrElse(1000)
    val maximumCapacity = config.getInt (prefix + ".maximum_capacity").getOrElse(10000)

    val collection = config.getString(prefix + ".collection").getOrElse(prefix)

    new MongoStage(
      database   = database,
      collection = collection,
      mongoStageSettings = MongoStageSettings(
        expirationPolicy = ExpirationPolicy(
          timeToIdle = Some(timeToIdle),
          timeToLive = Some(timeToLive),
          unit       = TimeUnit.MILLISECONDS
        ),
        maximumCapacity = maximumCapacity
      )
    )
  }

  private val variable_series        = newMongoStage("variable_series")
  private val variable_value_series  = newMongoStage("variable_value_series")

  private val variable_values        = newMongoStage("variable_values")
  private val variable_children      = newMongoStage("variable_children")
  private val path_children          = newMongoStage("path_children")

  val ListJValueOrdering = new Ordering[List[JValue]] {
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

  /** Aggregates the specified data. The object may contain multiple events or
   * just one.
   */
  def aggregate(token: Token, path: Path, time: DateTime, jobject: JObject, count: Long) = Future.async {
    // Keep track of parent/child relationships:
    path_children putAll addPathChildrenOfPath(token, path).patches

    val seriesCount = TimeSeries.point(timeGranularity, time, count)

    jobject.fields.foreach {
      case field @ JField(eventName, _) => 
        val event = JObject(field :: Nil)

        path_children += addChildOfPath(forTokenAndPath(token, path), "." + eventName)

        val valueReport = Report.ofValues(
          event = event,
          count = seriesCount,
          order = token.limits.order,
          depth = token.limits.depth,
          limit = token.limits.limit
        )

        variable_value_series putAll updateTimeSeries(token, path, valueReport).patches
        variable_values       putAll updateValues(token, path, valueReport.order(1)).patches


        val childCountReport = Report.ofChildren(
          event = event,
          count = count,
          order = 1,
          depth = token.limits.depth,
          limit = token.limits.limit
        )

        variable_children putAll updateChildren(token, path, childCountReport.order(1)).patches


        val childSeriesReport = Report.ofChildren(
          event = event,
          count = seriesCount,
          order = 1,
          depth = token.limits.depth,
          limit = token.limits.limit
        )

        variable_series putAll updateTimeSeries(token, path, childSeriesReport).patches
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
    extractValues(forTokenAndPath(token, path), path_children.collection) { (jvalue, _) =>
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
    getVariableSeries(token, path, variable, Periodicity.Eternity).map(_.total)
  }

  /** Retrieves a time series of counts of occurrences of the specified variable in a path */
  def getVariableSeries(token: Token, path: Path, variable: Variable, 
                        periodicity: Periodicity, start : Option[DateTime] = None, end : Option[DateTime] = None): 
                        Future[TimeSeriesType] = {

    variable.parent match {
      case None =>
        Future.lift(TimeSeries.empty[CountType](periodicity))

      case Some(parent) =>
        val lastNode = variable.name.nodes.last
        internalSearchSeries(variable_series.collection, token, path, Obs.ofChild(parent, lastNode), 
                             periodicity, start, end) 
    }
  }

  private def searchPeriod[T](start: Option[DateTime], end: Option[DateTime], f: Period => Future[T]): Future[List[T]] = {
    Future {
      (start <**> end) {
        (start, end) => timeSeriesEncoding.expand(start, end) map f
      } getOrElse {
        f(Period.Eternity) :: Nil
      }.toSeq: _*
    }
  }

  /** Retrieves a count of the specified observed state over the given time period */
  def searchCount(token: Token, path: Path, observation: Observation[HasValue],
                  start : Option[DateTime] = None, end : Option[DateTime] = None): Future[CountType] = {

    searchPeriod(start, end, searchSeries(token, path, observation, _: Period)) map {
      _.map(_.total).asMA.sum
    }
  }

  def searchSeries(token: Token, path: Path, observation: Observation[HasValue], period: Period): Future[TimeSeriesType] = {
    if (period == Period.Eternity) searchSeries(token, path, observation, Periodicity.Eternity, None, None)
    else searchSeries(token, path, observation, period.periodicity, Some(period.start), Some(period.end))
  }

  /** Retrieves a time series of counts of the specified observed state
   *  over the given time period.
   */
  def searchSeries(token: Token, path: Path, observation: Observation[HasValue], 
                   periodicity: Periodicity, start : Option[DateTime] = None, end : Option[DateTime] = None): 
                   Future[TimeSeriesType] = {

    internalSearchSeries(variable_value_series.collection, token, path, observation, periodicity, start, end)
  }

  private implicit def filterWhereObserved(filter: MongoFilter) = FilterWhereObserved(filter)
  private case class FilterWhereObserved(filter: MongoFilter) {
    /*
     * "where": {
     *   "variable1":  ".click.gender",
     *   "predicate1": "male"
     * }
     */
    def whereVariablesEqual[P <: Predicate : Decomposer](observation: Observation[P]): MongoFilter = {
      observation.toSeq.sortBy(_._1).zipWithIndex.foldLeft[MongoFilter](filter) {
        case (filter, ((variable, predicate), index)) =>
          filter & 
          JPath(".where.variable" + index)  === variable.serialize &
          JPath(".where.predicate" + index) === predicate.serialize
      }
    }

    def whereVariablesExist(variables: Seq[Variable]): MongoFilter = {
      variables.sorted.zipWithIndex.foldLeft[MongoFilter](filter) {
        case (filter, (variable, index)) =>
          filter & (JPath(".where.variable" + index) === variable.serialize)
      }
    }
  }

  type IntersectionResult[T] = SortedMap[List[JValue], T]

  def intersectCount(token: Token, path: Path, properties: List[VariableDescriptor],
                     start: Option[DateTime] = None, end: Option[DateTime] = None): Future[IntersectionResult[CountType]] = {

    searchPeriod(start, end, intersectSeries(token, path, properties, _: Period)) map {
      _.foldLeft(SortedMap.empty[List[JValue], CountType](ListJValueOrdering)) {
        case (total, partialResult) => partialResult.foldLeft(total) {
          case (total, (key, timeSeries)) => 
            total + (key -> total.get(key).map(_ |+| timeSeries.total).getOrElse(timeSeries.total))
        }
      } 
    }
  }

  def intersectSeries(token: Token, path: Path, properties: List[VariableDescriptor], period: Period): Future[IntersectionResult[TimeSeriesType]] = {
    if (period == Period.Eternity) intersectSeries(token, path, properties, Periodicity.Eternity, None, None)
    else intersectSeries(token, path, properties, period.periodicity, Some(period.start), Some(period.end))
  }

  def intersectSeries(token: Token, path: Path, properties: List[VariableDescriptor], 
                      granularity: Periodicity, start: Option[DateTime] = None, end: Option[DateTime] = None): 
                      Future[IntersectionResult[TimeSeriesType]] = {

    internalIntersectSeries(variable_value_series.collection, token, path, properties, granularity, start, end)
  }

  private def internalIntersectSeries(
      col: MongoCollection, token: Token, path: Path, variableDescriptors: List[VariableDescriptor], 
      granularity: Periodicity, start : Option[DateTime], end : Option[DateTime]): 
      Future[IntersectionResult[TimeSeriesType]] = { 

    val variables = variableDescriptors.map(_.variable)

    val futureHistograms = Future {
      variableDescriptors.map { 
        case VariableDescriptor(variable, maxResults, SortOrder.Ascending) =>
          getHistogramBottom(token, path, variable, maxResults).map(_.toMap)

        case VariableDescriptor(variable, maxResults, SortOrder.Descending) =>
          getHistogramTop(token, path, variable, maxResults).map(_.toMap)
      }: _*
    }

    futureHistograms.flatMap { histograms  => 
      val resultOrder = new scala.math.Ordering[List[JValue]] {
        override def compare(l1: List[JValue], l2: List[JValue]) = {
          val valueOrder = (l1 zip l2).zipWithIndex.foldLeft(0) {
            case (0, ((v1, v2), i)) => 
              val m = histograms(i)  
              variableDescriptors(i).sortOrder match {
                case SortOrder.Ascending  => -(m(v1) compare m(v2))
                case SortOrder.Descending =>   m(v1) compare m(v2)
              }
             
            case (x, _) => x
          }

          if (valueOrder == 0) ListJValueOrdering.compare(l1, l2) else valueOrder
        }
      }   

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
          internalSearchSeries(col, token, path, obs, granularity, start, end).map { result => 
            (variables.map(obsMap).map(_.value).toList -> result)
          }
        }.toSeq: _*
      } map {
        results => SortedMap(results: _*)(resultOrder)
      }
    }
  }

  private def valuesKeyFilter(token: Token, path: Path, variable: Variable) = {
    JPath("." + valuesId) === hashSignature(token.sig ++ path.sig ++ variable.sig)
  }

  /** Retrieves a histogram of the values a variable acquires over its lifetime.
   */
  private def getHistogramInternal(token: Token, path: Path, variable: Variable): Future[Map[HasValue, CountType]] = {
    getVariableLength(token, path, variable).flatMap { 
      case 0 =>
        (extractValues(valuesKeyFilter(token, path, variable), variable_values.collection) { 
          (jvalue, count) => (jvalue.deserialize[HasValue], count)
        }).map(_.toMap)

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

  private def deserializeTimeSeries(jv: JValue, granularity: Periodicity, start: Option[DateTime], end: Option[DateTime]): TimeSeriesType = {
    val startFloor = start.map(granularity.floor)
    val endCeil = end.map(granularity.ceil)
    (jv \ "counts") match {
      case JObject(fields) => 
        fields.foldLeft(TimeSeries.empty[CountType](granularity)) {
          case (series, JField(time, count)) => 
            val ltime = time.toLong 
            if (startFloor.forall(_.getMillis <= ltime) && endCeil.forall(_.getMillis > ltime)) 
               series + ((new DateTime(ltime), count.deserialize[CountType]))
            else series
        }

      case x => error("Unexpected serialization format for time series count data: " + renderNormalized(x))
    }
  }

  private def seriesKeyFilter[P <: Predicate : SignatureGen](token: Token, path: Path, observation: Observation[P],
                                                             period: Period, granularity: Periodicity) = {

    JPath("." + seriesId) === hashSignature(
      token.sig ++ path.sig ++ 
      period.sig ++ granularity.sig ++
      observation.sig
    )
  }

  private def internalSearchSeries[P <: Predicate: SignatureGen](
      col: MongoCollection, token: Token, path: Path, observation: Observation[P],
      granularity: Periodicity, start : Option[DateTime] = None, end : Option[DateTime] = None): Future[TimeSeriesType] = {

    val keyBuilder = seriesKeyFilter(token, path, observation, _: Period, granularity)
    val batchPeriodicity = timeSeriesEncoding.grouping(granularity)

    val intervalFilters = (start.map(batchPeriodicity.period), end.map(batchPeriodicity.period)) match {
      case (Some(start), Some(end)) => 
        if      (start == end)           keyBuilder(start) :: Nil
        else if (start.end == end.start) keyBuilder(start) :: keyBuilder(end) :: Nil
        else                             error("Query interval too large - too many results to return.")

      case (ostart, oend) => 
        ostart.orElse(oend).orElse((granularity == Periodicity.Eternity).option(Period.Eternity)).
        map(keyBuilder).toList
    }

    Future {
      intervalFilters.map(filter => database(selectOne(".counts").from(col).where(filter))): _*
    } map {
      _.flatten.foldLeft(TimeSeries.empty[CountType](granularity)) { 
        (cur, jv) => cur + deserializeTimeSeries(jv, granularity, start, end) 
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
        patches + addChildOfPath(forTokenAndPath(token, parent), child.elements.last + "/")
    }

    patches
  }

  /** Pushes the specified name onto a ".values" member of a document. This
   * function is used to keep track of the layout of the virtual file system.
   */
  private def addChildOfPath(filter: MongoFilter, child: String): (MongoFilter, MongoUpdate) = {
    val childField = MongoEscaper.encode(renderNormalized(child.serialize))

    val valuesUpdate = (".values." + childField) inc 1

    (filter -> valuesUpdate)
  }

  /** Creates patches to record variable observations.
   */
  private def updateValues[P <: Predicate : Decomposer, T](token: Token, path: Path, report: Report[P, T]): MongoPatches = {
    report.observationCounts.foldLeft(MongoPatches.empty) { 
      case (patches, (observation, _)) => observation.foldLeft(patches) {
        case (patches, (variable, predicate)) =>

          val predicateField = MongoEscaper.encode(renderNormalized(predicate.serialize))
          val valuesUpdate = (JPath(".values") \ JPathField(predicateField)) inc 1

          patches + (valuesKeyFilter(token, path, variable) -> valuesUpdate)
      }
    }
  }

  /** Creates patches to record variable observations.
   */
  private def updateChildren[P <: Predicate : Decomposer, T](token: Token, path: Path, report: Report[P, T]): MongoPatches = {
    report.observationCounts.foldLeft(MongoPatches.empty) { 
      case (patches, (observation, _)) => observation.foldLeft(patches) {
        case (patches, (variable, predicate)) =>

          val filterVariable = forTokenAndPath(token, path) & forVariable(variable)
          val predicateField = MongoEscaper.encode(renderNormalized(predicate.serialize))

          val valuesUpdate = (JPath(".values") \ JPathField(predicateField)) inc 1

          patches + (filterVariable -> valuesUpdate)
      }
    }
  }

  private def updateTimeSeries[P <: Predicate : Decomposer : SignatureGen](token: Token, path: Path, report: Report[P, TimeSeries[CountType]]): MongoPatches = {

    // aggregate time series up the scale of coarser granularities.   
    report.observationCounts.mapValues(_.aggregates).
    // build a map from period, contained granularity, and observation to time series at that granularity
    foldLeft(Map.empty[(Period, Periodicity, Observation[P]), TimeSeries[CountType]]) {
      case (m, (observation, multiSeries)) => multiSeries.foldLeft(m) {
        case (m, TimeSeries(granularity, values)) => values.foldLeft(m) {
          case (m, entry @ (start, _)) =>

            val batchKey = (timeSeriesEncoding.grouping(granularity).period(start), granularity, observation)
            m + (batchKey -> (m.getOrElse(batchKey, TimeSeries.empty[CountType](granularity)) + entry))
        }
      }
    }.foldLeft(MongoPatches.empty) {
      case (patches, ((period, granularity, observation), timeSeries)) => 
        val seriesKey = seriesKeyFilter(token, path, observation, period, granularity)

        patches + (seriesKey -> timeSeriesUpdater(".counts", timeSeries))
    }
  }

  def stop(): Future[Unit] =  for {
    _ <- variable_value_series.flushAll
    _ <- variable_series.flushAll
    _ <- variable_children.flushAll
    _ <- variable_values.flushAll
    _ <- path_children.flushAll
  } yield ()
}

object AggregationEngine {
  private val seriesId = "seriesId"
  private val valuesId = "valuesId"


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
    "variable_children" -> Map(
      "variable_query" -> (List("path", "accountTokenId", "variable"), false)
    ),
    "path_children" -> Map(
      "path_query" -> (List("path", "accountTokenId"), false)
    )
  )

  private def createIndices(database: MongoDatabase) = {
    val futures = for ((collection, indices) <- CollectionIndices; 
                       (indexName, (fields, unique)) <- indices) yield {
      database[JNothing.type] {
        if (unique) ensureUniqueIndex(indexName + "_index").on(fields.map(JPath(_)): _*).in(collection)
        else ensureIndex(indexName + "_index").on(fields.map(JPath(_)): _*).in(collection)
      }.toUnit
    }

    Future(futures.toSeq: _*)
  }

  def apply(config: ConfigMap, logger: Logger, database: MongoDatabase): Future[AggregationEngine] = {
    createIndices(database).map(_ => new AggregationEngine(config, logger, database))
  }
}
