package com.reportgrid.analytics

import blueeyes.concurrent.{Future, FutureDeliveryStrategySequential}
import blueeyes.persistence.mongo._
import blueeyes.persistence.cache.{Stage, ExpirationPolicy, CacheSettings}
import blueeyes.json.JsonAST._
import blueeyes.json.{JsonParser, JPath}
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._

import net.lag.configgy.ConfigMap
import net.lag.logging.Logger

import org.joda.time.{DateTime, DateTimeZone}

import java.util.concurrent.TimeUnit

import com.reportgrid.analytics.AggregatorImplicits._
import com.reportgrid.analytics.persistence.MongoSupport._

class AggregationEngine2(config: ConfigMap, logger: Logger, database: MongoDatabase) extends FutureDeliveryStrategySequential {
  val EarliestTime = new DateTime(0,             DateTimeZone.UTC)
  val LatestTime   = new DateTime(Long.MaxValue, DateTimeZone.UTC)

  type CountType          = Long
  type TimeSeriesType     = TimeSeries[CountType]

  type ChildReport        = Report[CountType, HasChild]
  val  ChildReportEmpty   = Report.empty[CountType, HasChild]

  type ValueReport        = Report[TimeSeriesType, HasValue]
  val  ValueReportEmpty   = Report.empty[TimeSeriesType, HasValue]

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
        initialCapacity = initialCapacity,
        maximumCapacity = maximumCapacity
      )
    ), collection)
  }

  private val (varSeriesS,      varSeriesC)       = newMongoStage("variable_series")
  private val (varValueSeriesS, varValueSeriesC)  = newMongoStage("variable_value_series")

  private val (varValueS,       varValueC)        = newMongoStage("variable_values")
  private val (varChildS,       varChildC)        = newMongoStage("variable_children")
  private val (pathChildS,      pathChildC)       = newMongoStage("path_children")

  private val DefaultAggregator = TimeSeriesAggregator.Default

  /** Aggregates the specified data. The object may contain multiple events or
   * just one.
   */
  def aggregate(token: Token, path: Path, time: DateTime, jobject: JObject, count: Long) = {
    Future.async {
      // Keep track of parent/child relationships:
      pathChildS ++= addPathChildrenOfPath(token, path).patches

      val accountPathFilter = forTokenAndPath(token, path)

      val seriesCount = DefaultAggregator.aggregate(time, count)

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

        varValueSeriesS ++= updateTimeSeries(accountPathFilter, valueReport).patches
        varValueS       ++= updateValues(accountPathFilter, valueReport).patches

        val childCountReport = Report.ofChildren(
          event = event,
          count = count,
          order = 1,
          depth = token.limits.depth,
          limit = token.limits.limit
        )

        varChildS ++= updateValues(accountPathFilter, childCountReport).patches

        val childSeriesReport = Report.ofChildren(
          event = event,
          count = seriesCount,
          order = 1,
          depth = token.limits.depth,
          limit = token.limits.limit
        )

        varSeriesS  ++= updateTimeSeries(accountPathFilter, childSeriesReport).patches
      }
    }
  }

  /** Retrieves children of the specified path & variable.
   */
  def getChildren(token: Token, path: Path, variable: Variable): Future[List[String]] = getChildren(token, path, Some(variable))

  /** Retrieves children of the specified path & possibly variable.
   */
  def getChildren(token: Token, path: Path, variable: Option[Variable] = None): Future[List[String]] = {
    val filter = forTokenAndPath(token, path)

    (variable match {
      case None =>
        extractValues(filter, pathChildC) { jvalue =>
          jvalue.deserialize[String]
        }

      case Some(variable) =>
        extractValues(filter & forVariable(variable), varChildC) { jvalue =>
          jvalue.deserialize[HasChild].child.toString
        }
    })
  }

  // TODO: Add distribution!!!!!!!

  /** Retrieves values of the specified variable.
   */
  def getValues(token: Token, path: Path, variable: Variable): Future[List[JValue]] = {
    val filter = forTokenAndPath(token, path)

    extractValues(filter & forVariable(variable), varValueC) { jvalue =>
      jvalue.deserialize[HasValue].value
    }
  }

  /** Retrieves a count of how many times the specified variable appeared in
   * an event.
   */
  def getVariableSeries(token: Token, path: Path, variable: Variable, periodicity: Periodicity, start_ : Option[DateTime] = None, end_ : Option[DateTime] = None): Future[TimeSeriesType] = {
    variable.parent match {
      case None =>
        Future.lift(TimeSeries.empty)

      case Some(parent) =>
        val lastNode = variable.name.nodes.last

        internalSearchSeries(varSeriesC, token, path, periodicity, Set((parent, HasChild(lastNode))), start_, end_)
    }
  }

  /** Retrieves a count of how many times the specified variable appeared in
   * an event.
   */
  def getVariableCount(token: Token, path: Path, variable: Variable): Future[CountType] = {
    getVariableSeries(token, path, variable, Periodicity.Eternity).map(_.total)
  }

  /** Retrieves a time series for the specified observed value of a variable.
   */
  def getValueSeries(token: Token, path: Path, variable: Variable, value: JValue, periodicity: Periodicity, start_ : Option[DateTime] = None, end_ : Option[DateTime] = None): Future[TimeSeriesType] = {
    searchSeries(token, path, periodicity, Set(variable -> HasValue(value)), start_, end_)
  }

  /** Retrieves a count for the specified observed value of a variable.
   */
  def getValueCount(token: Token, path: Path, variable: Variable, value: JValue): Future[Long] = {
    getValueSeries(token, path, variable, value, Periodicity.Eternity).map(_.total)
  }

  /** Searches time series to locate observations matching the specified criteria.
   */
  def searchSeries(token: Token, path: Path, periodicity: Periodicity, observation: Observation[HasValue],
    start_ : Option[DateTime] = None, end_ : Option[DateTime] = None): Future[TimeSeriesType] = {
    internalSearchSeries(varValueSeriesC, token, path, periodicity, observation, start_, end_)
  }

  /** Searches counts to locate observations matching the specified criteria.
   */
  def searchCount(token: Token, path: Path, observation: Observation[HasValue],
    start_ : Option[DateTime] = None, end_ : Option[DateTime] = None): Future[Long] = {
    searchSeries(token, path, Periodicity.Eternity, observation, start_, end_).map(_.total)
  }

  def stop(): Future[Unit] = Future.async {
    varValueSeriesS.stop
    varSeriesS.stop
    varChildS.stop
    varValueS.stop
    pathChildS.stop
  }

  /** Creates a bunch of patches to keep track of parent/child path relationships.
   * E.g. if you send "/foo/bar/baz", it will keep track of the following:
   *
   * "/foo" has child "bar"
   * "/foo/bar" has child "baz"
   */
  private def addPathChildrenOfPath(token: Token, path: Path): MongoPatches = {
    val patches = path.parentChildRelations.foldLeft(MongoPatches.empty) { (patches, tuple) =>
      val (parent, child) = tuple

      val (filter, update) = addChildOfPath(forTokenAndPath(token, parent), child.elements.last + "/")

      patches + (filter -> update)
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
  private def updateValues[T, P <: Predicate](filter: MongoFilter, report: Report[T, P])
    (implicit tsUpdater: (JPath, TimeSeriesType) => MongoUpdate, pDecomposer: Decomposer[P]): MongoPatches = {
    report.observationCounts.foldLeft(MongoPatches.empty) { (patches, tuple) =>
      val (observation, _) = tuple

      observation.foldLeft(patches) { (patches, tuple) =>
        val (variable, predicate) = tuple

        val filterVariable = filter & forVariable(variable)

        val predicateField = MongoEscaper.encode(renderNormalized(predicate.serialize))

        val valuesUpdate = (".values." + predicateField) inc 1

        patches + (filterVariable -> valuesUpdate)
      }
    }
  }

  private def updateTimeSeries[P <: Predicate](filter: MongoFilter, report: Report[TimeSeriesType, P])
    (implicit tsUpdater: (JPath, TimeSeriesType) => MongoUpdate, pDecomposer: Decomposer[P]): MongoPatches = {
    report.groupByOrder.flatMap { tuple =>
      val (order, report) = tuple

      report.groupByPeriod.map { tuple =>
        val (period, report) = tuple

        ((order, period), report)
      }
    }.foldLeft(MongoPatches.empty) { (patches, tuple) =>
      val ((order, period), report) = tuple

      val filterOrderPeriod = (filter & {
        ".order"  === order.serialize &
        ".period" === period.serialize
      })

      report.observationCounts.foldLeft(patches) { (patches, tuple) =>
        val (observation, count) = tuple

        val filterWhereClause = observation.foldLeft[MongoFilter](filterOrderPeriod) { (filter, tuple) =>
          val (variable, predicate) = tuple

          filter & {
            (".where." + variableToFieldName(variable)) === predicate.serialize
          }
        }

        val timeSeriesUpdate = tsUpdater(".count", count)

        patches + (filterWhereClause -> timeSeriesUpdate)
      }
    }
  }

  private def internalSearchSeries[P <: Predicate](col: MongoCollection, token: Token, path: Path, periodicity: Periodicity, observation: Observation[P],
    start_ : Option[DateTime] = None, end_ : Option[DateTime] = None)(implicit decomposer: Decomposer[P]): Future[TimeSeriesType] = {
    val filterTokenAndPath = forTokenAndPath(token, path)

    val start = start_.getOrElse(EarliestTime)
    val end   = end_.getOrElse(LatestTime)

    database {
      select(".count").from(col).where {
        filterTokenAndPath &
        JPath(".period.periodicity") === periodicity.serialize &
        JPath(".period.start")       >=  start.serialize &
        JPath(".period.start")        <  end.serialize &
        JPath(".order") === observation.size & {
          observation.foldLeft[MongoFilter](MongoFilterAll) { (filter, tuple) =>
            val (variable, predicate) = tuple

            filter & {
              JPath(".where." + variableToFieldName(variable)) === predicate.serialize
            }
          }
        }
      }
    }.map { results =>
      results.map { result =>
        (result \ "count").deserialize[TimeSeriesType]
      }.foldLeft[TimeSeriesType](TimeSeries.empty) { _ + _ }.fillGaps
    }
  }

  private def updateCount[P <: Predicate](filter: MongoFilter, report: Report[CountType, P])
    (implicit cUpdater: (JPath, CountType) => MongoUpdate, pDecomposer: Decomposer[P]): MongoPatches = {
    report.groupByOrder.foldLeft(MongoPatches.empty) { (patches, tuple) =>
      val (order, report) = tuple

      val filterOrder = (filter & {
        ".order"  === order.serialize
      })

      report.observationCounts.foldLeft(patches) { (patches, tuple) =>
        val (observation, count) = tuple

        val filterWhereClause = observation.foldLeft[MongoFilter](filterOrder) { (filter, tuple) =>
          val (variable, predicate) = tuple

          filter & {
            (".where." + variableToFieldName(variable)) === predicate.serialize
          }
        }

        val countUpdate = cUpdater(".count", count)

        patches + (filterWhereClause -> countUpdate)
      }
    }
  }

  private def extractValues[T](filter: MongoFilter, collection: MongoCollection)(extractor: JValue => T): Future[List[T]] = {
    database {
      selectOne(".values").from(collection).where {
        filter
      }
    }.map { option =>
      option match {
        case None => Nil

        case Some(result) =>
          (result \ "values").children.collect {
            case JField(name, count) =>
              val jvalue = JsonParser.parse(MongoEscaper.decode(name))

              extractor(jvalue)
          }
      }
    }
  }

  private def forTokenAndPath(token: Token, path: Path): MongoFilter = {
    (".accountTokenId" === token.accountTokenId) &
    (".path"           === path.toString)
  }

  private def forVariable(variable: Variable): MongoFilter = {
     ".variable" === variableToFieldName(variable)
  }

  private def variableToFieldName(variable: Variable): String = variable match {
    case Variable(JPath.Identity) => "id"

    case _ => MongoEscaper.encode((variable.serialize --> classOf[JString]).value)
  }

  private def fieldNameToVariable(name: String): Variable = name match {
    case "id" => Variable(JPath.Identity)

    case _ => JString(MongoEscaper.decode(name)).deserialize[Variable]
  }
}