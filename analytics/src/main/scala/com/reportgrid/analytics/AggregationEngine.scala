package com.reportgrid.analytics

import blueeyes._
import blueeyes.concurrent.{Future, FutureDeliveryStrategySequential}
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

class AggregationEngine private (config: ConfigMap, logger: Logger, database: MongoDatabase) extends FutureDeliveryStrategySequential {
  import AggregationEngine._

  val EarliestTime = new DateTime(0,             DateTimeZone.UTC)
  val LatestTime   = new DateTime(Long.MaxValue, DateTimeZone.UTC)


  type CountType          = Long
  type TimeSeriesType     = TimeSeries[CountType]

  type ChildReport        = Report[HasChild, CountType]
  val  ChildReportEmpty   = Report.empty[HasChild, CountType]

  type ValueReport        = Report[HasValue, TimeSeriesType]
  val  ValueReportEmpty   = Report.empty[HasValue, TimeSeriesType]

  private def newMongoStage(prefix: String): (MongoStage, MongoCollection) = {
    val timeToIdle      = config.getLong(prefix + ".time_to_idle_millis").getOrElse(10000L)
    val timeToLive      = config.getLong(prefix + ".time_to_live_millis").getOrElse(10000L)
    val initialCapacity = config.getInt (prefix + ".initial_capacity").getOrElse(1000)
    val maximumCapacity = config.getInt (prefix + ".maximum_capacity").getOrElse(10000)

    val collection = config.getString(prefix + ".collection").getOrElse(prefix)

    (new MongoStage(
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
    ), collection)
  }

  private val (varSeriesS,      varSeriesC)       = newMongoStage("variable_series")
  private val (varValueSeriesS, varValueSeriesC)  = newMongoStage("variable_value_series")

  private val (varValueS,       varValueC)        = newMongoStage("variable_values")
  private val (varChildS,       varChildC)        = newMongoStage("variable_children")
  private val (pathChildS,      pathChildC)       = newMongoStage("path_children")

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
  def aggregate(token: Token, path: Path, time: DateTime, jobject: JObject, count: Long) = {
    Future.async {
      // Keep track of parent/child relationships:
      pathChildS putAll addPathChildrenOfPath(token, path).patches

      val accountPathFilter = forTokenAndPath(token, path)

      val seriesCount = TimeSeries(time, count)

      val events = jobject.children.collect {
        case JField(eventName, properties) => (eventName, JObject(JField(eventName, properties) :: Nil))
      }

      events.foreach { tuple =>
        val (eventName, event) = tuple

        pathChildS += addChildOfPath(accountPathFilter, "." + eventName)

        val valueReport = Report.ofValues(
          event = event,
          count = seriesCount,
          order = token.limits.order,
          depth = token.limits.depth,
          limit = token.limits.limit
        )

        varValueSeriesS putAll updateTimeSeries(token, path, valueReport).patches
        varValueS       putAll updateValues(accountPathFilter, valueReport.order(1)).patches

        val childCountReport = Report.ofChildren(
          event = event,
          count = count,
          order = 1,
          depth = token.limits.depth,
          limit = token.limits.limit
        )

        varChildS putAll updateValues(accountPathFilter, childCountReport.order(1)).patches

        val childSeriesReport = Report.ofChildren(
          event = event,
          count = seriesCount,
          order = 1,
          depth = token.limits.depth,
          limit = token.limits.limit
        )

        varSeriesS putAll updateTimeSeries(token, path, childSeriesReport).patches
      }
    }
  }

  /** Retrieves children of the specified path &amp; variable.
   */
  def getChildren(token: Token, path: Path, variable: Variable): Future[List[String]] = getChildren(token, path, Some(variable))

  /** Retrieves children of the specified path &amp; possibly variable.
   */
  def getChildren(token: Token, path: Path, variable: Option[Variable] = None): Future[List[String]] = {
    val filter = forTokenAndPath(token, path)

    (variable match {
      case None =>
        extractValues(filter, pathChildC) { (jvalue, _) =>
          jvalue.deserialize[String]
        }

      case Some(variable) =>
        extractValues(filter & forVariable(variable), varChildC) { (jvalue, _) =>
          jvalue.deserialize[HasChild].child.toString
        }
    })
  }
  
  def getHistogram(token: Token, path: Path, variable: Variable): Future[Map[JValue, CountType]] = 
    getHistogramInternal(token, path, variable)

  def getHistogramTop(token: Token, path: Path, variable: Variable, n: Int): Future[List[(JValue, CountType)]] = 
    getHistogramInternal(token, path, variable).map(_.toList.sortBy(- _._2).take(n))

  def getHistogramBottom(token: Token, path: Path, variable: Variable, n: Int): Future[List[(JValue, CountType)]] = 
    getHistogramInternal(token, path, variable).map(_.toList.sortBy(_._2).take(n))

  /** Retrieves values of the specified variable.
   */
  def getValues(token: Token, path: Path, variable: Variable): Future[Iterable[JValue]] = 
    getHistogramInternal(token, path, variable).map(_.map(_._1))

  def getValuesTop(token: Token, path: Path, variable: Variable, n: Int): Future[List[JValue]] = 
    getHistogramTop(token, path, variable, n).map(_.map(_._1))

  def getValuesBottom(token: Token, path: Path, variable: Variable, n: Int): Future[List[JValue]] = 
    getHistogramBottom(token, path, variable, n).map(_.map(_._1))

  /** Retrieves the length of array properties, or 0 if the property is not an array.
   */
  def getVariableLength(token: Token, path: Path, variable: Variable): Future[Int] = {
    getChildren(token, path, variable).map { children =>
      children.filterNot(_.endsWith("/")).map(JPath(_)).foldLeft(0) {
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
        case (running, (value, count)) =>
          val number = value.deserialize[Double]

          running.update(number, count)
      }).statistics
    }
  }

  /** Retrieves a count of how many times the specified variable appeared in
   * an event.
   */
  def getVariableSeries(token: Token, path: Path, variable: Variable, periodicity: Periodicity, start : Option[DateTime] = None, end : Option[DateTime] = None): Future[TimeSeriesType] = {
    variable.parent match {
      case None =>
        Future.lift(TimeSeries.empty)

      case Some(parent) =>
        val lastNode = variable.name.nodes.last

        internalSearchSeries(varSeriesC, token, path, periodicity, Set((parent, HasChild(lastNode))), start, end)
    }
  }

  /** Retrieves a count of how many times the specified variable appeared in
   * an event.
   */
  def getVariableCount(token: Token, path: Path, variable: Variable): Future[CountType] = {
    getVariableSeries(token, path, variable, Periodicity.Eternity).map(_.total(Periodicity.Eternity))
  }

  /** Retrieves a time series for the specified observed value of a variable.
   */
  def getValueSeries(token: Token, path: Path, variable: Variable, value: JValue, periodicity: Periodicity, start : Option[DateTime] = None, end : Option[DateTime] = None): Future[TimeSeriesType] = {
    searchSeries(token, path, Set(variable -> HasValue(value)), periodicity, start, end)
  }

  /** Retrieves a count for the specified observed value of a variable.
   */
  def getValueCount(token: Token, path: Path, variable: Variable, value: JValue): Future[Long] = {
    getValueSeries(token, path, variable, value, Periodicity.Eternity).map(_.total(Periodicity.Eternity))
  }

  /** Searches time series to locate observations matching the specified criteria.
   */
  def searchSeries(token: Token, path: Path, observation: Observation[HasValue], periodicity: Periodicity, 
    start : Option[DateTime] = None, end : Option[DateTime] = None): Future[TimeSeriesType] = {
    internalSearchSeries(varValueSeriesC, token, path, periodicity, observation, start, end)
  }

  /** Searches counts to locate observations matching the specified criteria.
   */
  def searchCount(token: Token, path: Path, observation: Observation[HasValue],
    start : Option[DateTime] = None, end : Option[DateTime] = None): Future[CountType] = {
    searchSeries(token, path, observation, Periodicity.Eternity,  start, end).map(_.total(Periodicity.Eternity))
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
    intersectSeries(token, path, properties, Periodicity.Eternity, start, end).map { series => 
      import series.ordering
      series.map {
        case (k, v) => (k, v.total(Periodicity.Eternity))
      }
    }
  }

  def intersectSeries(token: Token, path: Path, properties: List[VariableDescriptor], 
                      periodicity: Periodicity, start: Option[DateTime] = None, end: Option[DateTime] = None): Future[IntersectionResult[TimeSeriesType]] = {
    internalIntersectSeries(varValueSeriesC, token, path, properties, periodicity, start, end)
  }

  private def internalIntersectSeries[P <: Predicate](
      col: MongoCollection, token: Token, path: Path, variableDescriptors: List[VariableDescriptor], 
      granularity: Periodicity, start : Option[DateTime], end : Option[DateTime]): Future[IntersectionResult[TimeSeriesType]] = { 
    val variables = variableDescriptors.map(_.variable)

    val histograms = Future(variableDescriptors.map { 
      case VariableDescriptor(variable, maxResults, SortOrder.Ascending) =>
        getHistogramBottom(token, path, variable, maxResults).map(_.toMap)

      case VariableDescriptor(variable, maxResults, SortOrder.Descending) =>
        getHistogramTop(token, path, variable, maxResults).map(_.toMap)
    }: _*)

    histograms.flatMap { hist => 
      implicit def ordering: scala.math.Ordering[List[JValue]] = new scala.math.Ordering[List[JValue]] {
        override def compare(l1: List[JValue], l2: List[JValue]) = {
          val valueOrder = (l1 zip l2).zipWithIndex.foldLeft(0) {
            case (0, ((v1, v2), i)) => 
              val m = hist(i)  
              variableDescriptors(i).sortOrder match {
                case SortOrder.Ascending  => 
                  -(m(v1) compare m(v2))

                case SortOrder.Descending => 
                  m(v1) compare m(v2)
              }
             
            case (x, _) => x
          }

          if (valueOrder == 0) ListJValueOrdering.compare(l1, l2) else valueOrder
        }
      }


      val aggregator = implicitly[AbelianGroup[TimeSeriesType]]
      
      val filterBuilder = (period: Period) => {
          (forTokenAndPath(token, path) & 
          (".order" === variableDescriptors.length) &
          (".period.periodicity" === period.periodicity.serialize) &
          (".period.start" === period.start.serialize) &
          (".period.end" === period.end.serialize) &
          (".granularity" === granularity.serialize)).
          whereVariablesExist(variableDescriptors.map(_.variable))
      }

      Future {
        intervalFilters(granularity, start, end, filterBuilder) map {
          filter => database(select(".counts", ".where").from(col).where(filter ->- {f => println(renderNormalized(f.filter))}))
        }: _*
      } map {
        _.flatten.foldLeft(SortedMap.empty[List[JValue], TimeSeriesType]) { 
          case (m, result) =>
            // generate the key for the count in the results
            val values: List[JValue] = variableDescriptors.sortBy(_.variable).zipWithIndex.map { 
              case (vd, i) => (
                variables.indexOf(result.get(JPath(".where.variable" + i)).deserialize[Variable]), 
                result.get(JPath(".where.predicate" + i))
              )
            }.sortBy(_._1).map(_._2).toList

            // ensure that all the variables are within the set of values selected by
            // the histogram that is used for sorting.
            if (values.zipWithIndex.forall { case (v, i) => hist(i).isDefinedAt(v) }) {
              val count = deserializeTimeSeries(result, granularity, start, end)
              m + (values -> (m.getOrElse(values, TimeSeries.empty[CountType]) + count))
            } else m
        }
      }
    }
  }

  /** Retrieves a histogram of the values a variable acquires over its lifetime.
   */
  private def getHistogramInternal(token: Token, path: Path, variable: Variable): Future[Map[JValue, CountType]] = {
    getVariableLength(token, path, variable).flatMap { 
      case 0 =>
        (extractValues(forTokenAndPath(token, path) & forVariable(variable), varValueC) { 
          (jvalue, count) => (jvalue.deserialize[HasValue].value, count)
        }).map(_.toMap)

      case length =>
        Future((0 until length).map { index =>
          getHistogramInternal(token, path, Variable(variable.name \ JPathIndex(index)))
        }: _*).map { results =>
          results.foldLeft(Map.empty[JValue, CountType]) {
            case (all, cur) => all <+> cur // MapMonoid[JValue, CountType].append(all, cur)
          }
        }
    }    
  }

  private def intervalFilters(granularity: Periodicity, start: Option[DateTime], end: Option[DateTime], filterBuilder: Period => MongoFilter): List[MongoFilter] = {
    val batchPeriodicity = PeriodicityBatches(granularity)
    val batchStartPeriod = start.map(batchPeriodicity.period)
    val batchEndPeriod =   end.map(batchPeriodicity.period)

    (batchStartPeriod, batchEndPeriod) match {
      case (Some(start), Some(end)) => 
        if (start == end) filterBuilder(start) :: Nil
        else if (start.end == end.start) filterBuilder(start) :: filterBuilder(end) :: Nil
        else error("Query period too large; too many results to return.")

      case (ostart, oend) => List(
        ostart.orElse(oend).orElse(if (granularity == Periodicity.Eternity) Some(Period.Eternity) else None).map(filterBuilder) getOrElse {
          error("For queries with granularity other than eternity, start and end must be specified.")
        }
      )
    }
  }

  private def deserializeTimeSeries(jv: JValue, granularity: Periodicity, start: Option[DateTime], end: Option[DateTime]): TimeSeriesType = {
    (jv \ "counts") match {
      case JObject(fields) => fields.foldLeft(TimeSeries.empty[CountType]) {
        case (series, JField(time, count)) => 
          val ltime = time.toLong 
          if (start.forall(_ <= ltime) && end.forall(_ > ltime)) 
            series + Tuple2(granularity.period(new DateTime(ltime)), count.deserialize[CountType])
          else series
      }

      case x => error("Unexpected serialization format for time series count data: " + renderNormalized(x))
    }
  }

  private def internalSearchSeries[P <: Predicate: Decomposer](col: MongoCollection, token: Token, path: Path, granularity: Periodicity, observation: Observation[P],
                                                               start : Option[DateTime] = None, end : Option[DateTime] = None): Future[TimeSeriesType] = {
    val filterBuilder = timeSeriesKeyFilter(token, path, observation.size, _: Period, granularity, observation)

    Future {
      intervalFilters(granularity, start, end, filterBuilder).map {
        filter => database(selectOne(".counts").from(col).where(filter)) 
      }: _*
    } map {
      _.flatten.foldLeft(TimeSeries.empty[CountType]) { 
        (cur, jv) =>  cur + deserializeTimeSeries(jv, granularity, start, end) 
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
  private def updateValues[P <: Predicate : Decomposer, T](filter: MongoFilter, report: Report[P, T]): MongoPatches = {
    report.observationCounts.foldLeft(MongoPatches.empty) { 
      case (patches, (observation, _)) => observation.foldLeft(patches) {
        case (patches, (variable, predicate)) =>

          val filterVariable = filter & forVariable(variable)
          val predicateField = MongoEscaper.encode(renderNormalized(predicate.serialize))

          val valuesUpdate = (JPath(".values") \ JPathField(predicateField)) inc 1

          patches + (filterVariable -> valuesUpdate)
      }
    }
  }

  private def timeSeriesKeyFilter[P <: Predicate: Decomposer](token: Token, path: Path, order: Int, period: Period, granularity: Periodicity, observation: Observation[P]) = {
    def sha1(bytes : Array[Byte]) : String = {
      val hash = java.security.MessageDigest.getInstance("SHA-1")
      hash.reset()
      hash.update(bytes)
      hash.digest().map(0xFF & _).map { "%02x".format(_) }.foldLeft(""){_ + _}
    }

    val orderPeriodFilter = forTokenAndPath(token, path) & 
                            (".order"  === order.serialize) &
                            (".period" === period.serialize) &
                            (".granularity" === granularity.serialize) 

    val filterWhereClause = orderPeriodFilter.whereVariablesEqual(observation)

    JPath(".updateKey") === sha1(renderNormalized(filterWhereClause.filter).toString.getBytes)
  }


  private def updateTimeSeries[P <: Predicate : Decomposer](token: Token, path: Path, report: Report[P, TimeSeriesType]): MongoPatches = {

    val countUpdate = timeSeriesUpdater[CountType]
    val up = MongoUpdateBuilder(_)

    def observationUpdate[P <: Predicate : Decomposer](observation: Observation[P]): MongoUpdate = {
      observation.toSeq.sortBy(_._1).zipWithIndex.foldLeft[MongoUpdate](MongoUpdateNothing) {
        case (update, ((variable, predicate), index)) =>
          update & 
          (up(".where.variable" + index)  set variable.serialize) &
          (up(".where.predicate" + index) set predicate.serialize)
      }
    }

    val baseUpdate = (up(".accountTokenId") set token.accountTokenId.serialize) &
                     (up(".path") set path.serialize)

    report.groupByOrder.view.foldLeft(MongoPatches.empty) { 
      case (patches, (order, report)) => report.partition(PeriodicityBatches).foldLeft(patches) { 
        case (patches, (BatchKey(period, granularity, observation), timeSeries)) => 
          
          val orderPeriodUpdate = baseUpdate & 
                                  (up(".order") set order.serialize) &
                                  (up(".period") set period.serialize)  & 
                                  (up(".granularity") set granularity.serialize)

          val updateKey = timeSeriesKeyFilter(token, path, order, period, granularity, observation)

          patches + (updateKey -> (orderPeriodUpdate & 
                                   observationUpdate(observation) & 
                                   countUpdate(".counts", timeSeries)))
      }
    }
  }

  private def updateCount[P <: Predicate](filter: MongoFilter, report: Report[P, CountType])
    (implicit cUpdater: (JPath, CountType) => MongoUpdate, pDecomposer: Decomposer[P]): MongoPatches = {
    report.groupByOrder.foldLeft(MongoPatches.empty) { (patches, tuple) =>
      val (order, report) = tuple

      val filterOrder = (filter & {
        ".order"  === order.serialize
      })

      report.observationCounts.foldLeft(patches) {
        case (patches, (observation, count)) =>
          val filterWhereClause = filterOrder.whereVariablesEqual(observation)
          val countUpdate = cUpdater(".count", count)

          patches + (filterWhereClause -> countUpdate)
      }
    }
  }

  def stop(): Future[Unit] =  for {
    _ <- varValueSeriesS.flushAll
    _ <- varSeriesS.flushAll
    _ <- varChildS.flushAll
    _ <- varValueS.flushAll
    _ <- pathChildS.flushAll
  } yield ()
}

object AggregationEngine extends FutureDeliveryStrategySequential {
  import Periodicity._
  val PeriodicityBatches: Periodicity => Periodicity = Map(
    Second -> Day,
    Minute -> Month,
    Hour -> Year,
    Day -> Year,
    Week -> Eternity,
    Month -> Eternity,
    Year -> Eternity,
    Eternity -> Eternity
  )

  private val CollectionIndices = Map(
    "variable_series" -> Map(
      "val_series_query" -> ("path" :: "accountTokenId" :: "order" :: "period" ::
                            ((0 to 9).flatMap(i => List("where.variable" + i, "where.predicate" + i)).toList))
    ),
    "variable_value_series" -> Map(
      "updateKey" -> List("updateKey"),
      "var_val_series_query" -> ("accountTokenId" :: "granularity" :: "order" :: "path" :: "period" ::  
                                ((0 to 9).flatMap(i => List("where.variable" + i, "where.predicate" + i)).toList))
    ),
    "variable_values" -> Map(
      "variable_query" -> List("path", "accountTokenId", "variable")
    ),
    "variable_children" -> Map(
      "variable_query" -> List("path", "accountTokenId", "variable")
    ),
    "path_children" -> Map(
      "path_query" -> List("path", "accountTokenId")
    )
  )

  private def createIndices(database: MongoDatabase) = {
    val futures = for ((collection, indices) <- CollectionIndices; (indexName, fields) <- indices) yield {
      database[JNothing.type] {
        ensureIndex(indexName + "_index").on(fields.map(JPath(_)): _*).in(collection)
      }.toUnit
    }

    Future(futures.toSeq: _*)
  }

  def apply(config: ConfigMap, logger: Logger, database: MongoDatabase): Future[AggregationEngine] = {
    createIndices(database).map(_ => new AggregationEngine(config, logger, database))
  }
}
