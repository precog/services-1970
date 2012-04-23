package com.reportgrid.analytics

import external.Jessup

import com.reportgrid.common.HashFunction
import com.reportgrid.common.Sha1HashFunction

import blueeyes._
import blueeyes.concurrent._
import blueeyes.health.HealthMonitor
import blueeyes.persistence.mongo._
import blueeyes.persistence.mongo.MongoFilterImplicits._
import blueeyes.persistence.cache.{Stage, ExpirationPolicy, CacheSettings}
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultOrderings._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.JodaSerializationImplicits._
import blueeyes.util.Clock
import blueeyes.util.ClockSystem

import com.reportgrid.analytics._
import com.reportgrid.analytics.persistence.MongoSupport._
import com.reportgrid.ct._

import java.util.concurrent.TimeUnit

import net.lag.configgy.ConfigMap
import com.weiglewilczek.slf4s.Logger

import org.joda.time.Instant

import scala.collection.SortedMap
import scalaz.Scalaz._
import scalaz.NonEmptyList
import scalaz.Validation
import Future._
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
  def put(t: (MongoFilter, MongoUpdate)): Int = this.put(t._1, t._2)
  def put(filter: MongoFilter, update: MongoUpdate): Int = {
    stage.put(filter & collection, update)
    1
  }

  def putAll(filters: Iterable[(MongoFilter, MongoUpdate)]): Int = {
    stage.putAll(filters.map(((_: MongoFilter).&(collection)).first[MongoUpdate]))
    filters.size
  }
}

class AggregationEngine private (config: ConfigMap, val logger: Logger, val eventsdb: Database, val indexdb: Database, clock: Clock, val healthMonitor: HealthMonitor)(implicit hashFunction: HashFunction = Sha1HashFunction) {
  import AggregationEngine._
  val maxIndexedOrder = 0

  private def AggregationStage(prefix: String): AggregationStage = {
    val timeToIdle      = config.getLong(prefix + ".time_to_idle_millis").getOrElse(10000L)
    val timeToLive      = config.getLong(prefix + ".time_to_live_millis").getOrElse(10000L)
    val initialCapacity = config.getInt (prefix + ".initial_capacity").getOrElse(1000)
    val maximumCapacity = config.getInt (prefix + ".maximum_capacity").getOrElse(10000)

    val collection = config.getString(prefix + ".collection").getOrElse(prefix)

    new AggregationStage(
      collection = collection,
      stage = MongoStage(
        database   = indexdb,
        mongoStageSettings = MongoStageSettings(
          expirationPolicy = ExpirationPolicy(
            timeToIdle = Some(timeToIdle),
            timeToLive = Some(timeToLive),
            timeUnit   = TimeUnit.MILLISECONDS
          ),
          maximumCapacity = maximumCapacity
        )//,
        //healthMonitor
      )
    )
  }

  // Helpers for map/reduce
  val matchesAnonLocFunc = """
// obj = "#location" value, prefixes = array of prefix strings
  function matchesAnonLoc(obj, prefixes) {
      if (typeof obj == 'string' && prefixes.some(function(prefix) { return obj.substring(0, prefix.length) == prefix })) { // seriously? No startsWith?
          return true;
      }
      if (typeof obj == 'object') {
          for (var prop in obj) {
              if (matchesAnonLoc(obj[prop], prefixes)) {
                  return true;
              }
          }
      }
      return false;
  }
"""

  private def jvalToLong(jv : JValue) = jv match {
    case JInt(bd) => bd.longValue
    case JDouble(d) => d.toLong
    case invalid => sys.error("Can't convert " + invalid + " to Long")
  }

  private def jvalToDouble(jv : JValue) = jv match {
    case JInt(bd) => bd.doubleValue
    case JDouble(d) => d
    case invalid => sys.error("Can't convert " + invalid + " to Double")
  }


  private val variable_children         = AggregationStage("variable_children")
  private val path_children             = AggregationStage("path_children")

  private val path_tags                 = AggregationStage("path_tags")
  private val hierarchy_children        = AggregationStage("hierarchy_children")

  def flushStages = List(variable_children, path_children).map(_.stage.flushAll).sequence.map(_.sum)

  val events_collection: MongoCollection = config.getString("events.collection", "events")

  def store(token: Token, path: Path, eventName: String, eventBody: JValue, tagResults: Tag.ExtractionResult, count: Int, rollup: Int, reprocess: Boolean) = {
    if (token.limits.lossless) {
      def withTagResults[A](f: Seq[Tag] => Future[A]): Future[A] = tagResults match {
        case Tag.Tags(tf) => tf.flatMap(f)
        case _ => f(Nil)
      }

      withTagResults { (tags: Seq[Tag]) =>
        val instant = tags.collect{ case Tag("timestamp", TimeReference(_, instant)) => instant }.headOption.getOrElse(clock.instant)

        val min_rollup = path.length - rollup

        val record = JObject(
          JField("token",     token.tokenId.serialize) ::
          JField("accountTokenId", token.accountTokenId.serialize) ::
          JField("path",      path.serialize) ::
          JField("event",     JObject(JField("name", eventName) :: JField("data", eventBody) :: Nil)) :: 
          JField("tags",      tags.map(_.serialize).reduceLeftOption(_ merge _).serialize) ::
          JField("count",     count.serialize) ::
          JField("timestamp", instant.serialize) :: 
          JField("min_ru",    min_rollup.serialize) :: 
          (if (reprocess) JField("reprocess", true.serialize) :: Nil else Nil)
        )

        eventsdb(MongoInsertQuery(events_collection, List(record))) 
      } 
    } else {
      Future.sync(())
    }
  }

  def retrieveEventsFor(filter : MongoFilter, fields : Option[List[JPath]] = None) : Future[scala.collection.IterableView[JObject, Iterator[JObject]]] = {
    logger.trace("Fetching events for filter " + compact(render(filter.filter)) + ", with fields: " + fields.map(_.mkString(",")).getOrElse("all"))
    fields.map {
      fieldsToGet => eventsdb(select(fieldsToGet : _*).from(events_collection).where(filter))
    }.getOrElse(eventsdb(selectAll.from(events_collection).where(filter))).map {
      events => {
        events.map(unescapeEventBody).view.asInstanceOf[scala.collection.IterableView[JObject, Iterator[JObject]]]
      }
    }
  }

  def withTagResults[A](tagResults: Tag.ExtractionResult)(f: Seq[Tag] => Future[A]): Future[ValidationNEL[String, A]] = {
    tagResults match {
      case Tag.Tags(tf)       => tf.flatMap(f).map(_.success[NonEmptyList[String]])
      case Tag.Skipped        => f(Nil).map(_.success[NonEmptyList[String]])
      case Tag.Errors(errors) => Future.sync(errors.map(_.toString).fail[A])
    }
  }

  def aggregate(token: Token, path: Path, eventName: String, tagResults: Tag.ExtractionResult, eventBody: JObject, count: Int): Future[Validation[NonEmptyList[String], Long]] = {
    withTagResults(tagResults) { 
      aggregate(token, path, eventName, _, eventBody, count)
    }
  }

  /** 
   * Add the event data for the specified event to the database
   */
  def aggregate(token: Token, path: Path, eventName: String, allTags: Seq[Tag], eventBody: JObject, count: Int): Future[Long] = Future.async {
    import token.limits.{order, depth, limit}

    val tags = allTags.take(token.limits.tags)
    val event = JObject(JField(eventName, eventBody) :: Nil)

    val (finiteObs, infiniteObs) = JointObservations.ofValues(event, 1 /*order*/, depth, limit)
    val finiteOrder1 = finiteObs.filter(_.order == 1)
    val childObservations = JointObservations.ofChildren(event, 1)
    val childSeriesReport = Report(tags, (JointObservations.ofInnerNodes(event, 1) ++ finiteOrder1 ++ infiniteObs.map(JointObservation(_))).map(_.of[Observation]))

    // Keep track of parent/child relationships:
    (path_children putAll addPathChildren(token, path).patches) +
    (path_children put addChildOfPath(forTokenAndPath(token, path), "." + eventName)) + 
    (path_tags put addPathTags(token, path, allTags)) +
    (hierarchy_children putAll addHierarchyChildren(token, path, allTags).patches) +
    (variable_children putAll variableChildrenPatches(token, path, childObservations.flatMap(_.obs), count).patches)// + 
  }

  /** Retrieves children of the specified path &amp; variable.  */
  def getVariableChildren(token: Token, path: Path, variable: Variable, tagTerms: Seq[TagTerm] = Nil): Future[List[(HasChild, Long)]] = {
    logger.debug("Retrieving variable children for " + path + " : " + variable)

    if (tagTerms.size == 0) {
      extractValues(forTokenAndPath(token, path) & forVariable(variable), variable_children.collection) { (jvalue, count) =>
        (HasChild(variable, jvalue match {
          case JString(str) => JPathField(str.replaceAll("^\\.+", "")) // Handle any legacy data (stores prefixed dot)
          case JInt(index)  => JPathIndex(index.toInt)
        }), count.toLong)
      }.map { 
        // Sum up legacy and new keys with the same name
        vals => vals.groupBy { case (name,count) => name }.map { case (name,values) => (name, values.map(_._2).sum) }.toList
      }
    } else {
      logger.debug("Using raw events for variable children")
      getMRVariableChildren(token, path, variable, tagTerms)
    }
  }

  def getMRVariableChildren(token: Token, path: Path, variable: Variable, tagTerms: Seq[TagTerm]): Future[List[(HasChild, Long)]] = {
    val emitter = """
  var c = {};
  for (var prop in this%s) {
    if (prop[0] != '#') {
      c[prop] = 1;
    }
  }
  emit("children", { children : c });
""".format(JPath(".event.data") \ variable.name.tail)

    val reducer = """
function(key, values) {
    var c = {};
    for (var i = 0; i < values.length; i++) {
        for (var prop in values[i].children) {
          if (typeof c[prop] == 'undefined') {
            c[prop] = values[i]['children'][prop];
          } else {
            c[prop] += values[i]['children'][prop];
          }
        }
    }
    return {children : c};
}
"""

    queryMapReduce(token, path, variable, tagTerms, Set(), emitter, reducer) { output =>
      eventsdb(selectAll.from(output.outputCollection)).map { 
        objs => {
          val results = objs.toList.map {
            jo => jo("value")("children") match {
              case JObject(fields) => fields.map {
                case JField(name, JInt(i))    => (HasChild(variable, JPathField(name)), i.longValue)
                case JField(name, JDouble(d)) => (HasChild(variable, JPathField(name)), d.toLong)
                case invalid                  => logger.error("Invalid result returned from getChildren map/reduce: " + invalid); sys.error("Invalid result")
              }
              case invalid         => logger.error("Invalid result returned from getChildren map/reduce: " + invalid); sys.error("Invalid result")
            }
          }
          logger.trace("MapReduce Results = " + results)
          results.flatten
        }
      }
    }
  }

  /** Retrieves children of the specified path.  */
  def getPathChildren(token: Token, path: Path): Future[List[String]] = {
    extractChildren(forTokenAndPath(token, path), path_children.collection) { (jvalue, _) =>
      jvalue.deserialize[String]
    }
  }

  def getPathTags(token: Token, path: Path): Future[List[String]] = {
    indexdb(selectAll.from(path_tags.collection).where(forTokenAndPath(token, path))) map { results =>
      val res = results.map(_ \ "tags") flatMap {
        case r @ JObject(fields) => fields.map(_.name)
        case _ => Nil
      }
      
      res.toList
    }
  }

  def getHierarchyChildren(token: Token, path: Path, tagName: String, jpath: JPath) = {
    val dataPath = JPath(".values") \ jpath
    indexdb(select(dataPath).from(hierarchy_children.collection).where(forHierarchyTag(token, path, tagName))) map { results =>
      results.map(_(dataPath)).flatMap {
        case JObject(fields) => fields.map(_.name).filter(_ != "#count")
        case _ => Nil
      }
    }
  }

  // Cannot be used for queries with anon locs
  private def queryAggregation[T](token: Token, path: Path, variable: Variable, tagTerms: Seq[TagTerm], where: Set[HasValue], pipeline: List[JObject])(finalizer: Option[JObject] => Future[T]): Future[T] = {
    val fullFilterParts = List(Some(rawEventsBaseFilter(token, path)), 
                               rawEventsTagsFilter(tagTerms), 
                               Some(pathRollupFilter(path)),
                               eventNameFilter(variable),
                               variableExistsFilter(variable),
                               constraintFilter(where))
    
    logger.trace("Aggregation: Filter components = " + fullFilterParts.flatten)

    val fullFilter = fullFilterParts.flatten.reduceLeft(_ & _)
    
    val finalPipe = JArray(JObject(List(JField("$match", fullFilter.filter))) :: pipeline)

    logger.trace("Final pipe = " + render(finalPipe))

    eventsdb(aggregation(finalPipe).from(events_collection)).flatMap(finalizer)
  }

  private def queryMapReduce[T](token: Token, path: Path, variable: Variable, tagTerms: Seq[TagTerm], where: Set[HasValue], emitter: String, reducer: String)(finalizer: MapReduceOutput => Future[T]): Future[T] = {
    val fullFilterParts = List(Some(rawEventsBaseFilter(token, path)), 
                               rawEventsTagsFilter(tagTerms), 
                               Some(pathRollupFilter(path)),
                               eventNameFilter(variable),
                               variableExistsFilter(variable),
                               constraintFilter(where))
    
    logger.trace("MR: Filter components = " + fullFilterParts.flatten)

    val fullFilter = fullFilterParts.flatten.reduceLeft(_ & _)

    // Results go to temp collection. This works around a MongoDB Java driver bug as well as allowing for result sets > 16MB
    val resultCollection = ("mapreduce" + java.util.UUID.randomUUID.toString).replace("-","")

    logger.trace("MR: Sending results to " + resultCollection)

    val anonLocationPrefixes : Seq[String] = tagTerms.collect {
      case HierarchyLocationTerm(tagName, Hierarchy.AnonLocation(path)) => path.path
    }

    val mapHelpers = if (anonLocationPrefixes.isEmpty) "" else matchesAnonLocFunc

    val emitterClause = if (anonLocationPrefixes.isEmpty) emitter else "if (matchesAnonLoc(this['tags']['#location'], %s) { %s }".format(anonLocationPrefixes.mkString("[\"", "\",\"", "\"])"), emitter)
    
    val mapFunc = """
function() {
  if(!Array.isArray) {  
    Array.isArray = function (arg) {  
      return Object.prototype.toString.call(arg) == '[object Array]';  
    };  
  }  
%s

  %s;
}
""".format(mapHelpers, emitterClause)

    logger.trace("MR: MapFunc = " + mapFunc)
    logger.trace("MR: Reducer = " + reducer)
    logger.trace("MR: Filter " + compact(render(fullFilter.filter)))
    
    eventsdb(mapReduce(mapFunc, reducer).from(events_collection).where(fullFilter).into(resultCollection)).flatMap {
      output => {
        logger.trace("MR complete on " + resultCollection + " with result: " + output.status)
        logger.trace("MR running finalizer for " + resultCollection)
        val ret = finalizer(output)
        //output.drop()
        logger.trace("MR finalizer complete for " + resultCollection)
        ret
      }
    }
  }

  def getAggrHistogram(token: Token, path: Path, variable: Variable, tagTerms : Seq[TagTerm], additionalConstraints: Set[HasValue] = Set.empty[HasValue]): Future[Map[JValue, CountType]] = {
    if (hasAnonLocs(tagTerms)) {
      sys.error("Not yet!") //getMRHistogram(token, path, variable, tagTerms, additionalConstraints)
    } else {
      val variableName = (JPath(".event.data") \ variable.name.tail).toString.drop(1) // Drop leading dot

//      import blueeyes.json.JsonDSL._
//
//      val projection : JObject = ("$project", (variableName, 1))
//      val grouping   : JObject = ("$group", ("_id", "$" + variableName) ~ ("count", ("$sum", 1)))

      val pipeline = List(JObject(List(JField("$project", JObject(List(JField(variableName, JInt(1))))),
                                       JField("$group", JObject(List(JField("_id", "$" + variableName),
                                                                     JField("count", JObject(List(JField("$sum", JInt(1)))))))))))

      queryAggregation(token, path, variable, tagTerms, additionalConstraints, pipeline)  {
        case Some(result) => result("result") match {
          case JArray(objs) => Future.sync(objs.toList.map {
            jo => {
              val key = jo("_id")
              val count: Long = jvalToLong(jo("count"))
              (key,count)
            }
          }.toMap)
        }
      }
    }
  }

  def getHistogram(token: Token, path: Path, variable: Variable, tagTerms : Seq[TagTerm], additionalConstraints: Set[HasValue] = Set.empty[HasValue]): Future[Map[JValue, CountType]] = {

    logger.trace("Map/reduce for histogram on " + variable + " with tagTerms = " + tagTerms + " and add'l constraints: " + additionalConstraints)

    val emitter = """
    if (Array.isArray(this%1$s)) {
      for (var index in this%1$s) {
        if (this%1$s[index] != null) {
          emit(this%1$s[index], 1);
        }
      }
    } else {
      if (this%1$s != null) {
        emit(this%1$s, 1);
      }
    }
""".format(JPath(".event.data") \ variable.name.tail)

    logger.trace("Getting histogram with emitter: " + emitter)

    val reduceFunc = """
function(key, values) {
    var count = 0;
    for (var i = 0; i < values.length; i++) {
        count += values[i];
    }
    return count;
}
"""

    queryMapReduce(token, path, variable, tagTerms, additionalConstraints, emitter, reduceFunc) { output =>
      logger.trace("Fetching from " + output.outputCollection)
      eventsdb(selectAll.from(output.outputCollection)).map { 
        objs => {
          val resultMap = objs.toList.map {
            jo => {
              val key = jo("_id")
              val count: Long = jvalToLong(jo("value"))
              (key,count)
            }
          }.toMap
          logger.trace("MapReduce Results = " + resultMap)
          resultMap
        }
      }
    }
  }
  

  def getHistogramTop(token: Token, path: Path, variable: Variable, n: Int, tagTerms : Seq[TagTerm], additionalConstraints: Set[HasValue] = Set.empty[HasValue]): Future[ResultSet[JValue, CountType]] = 
    getHistogram(token, path, variable, tagTerms, additionalConstraints).map(v => v.toList.sortBy(- _._2).take(n))

  def getHistogramBottom(token: Token, path: Path, variable: Variable, n: Int, tagTerms : Seq[TagTerm], additionalConstraints: Set[HasValue] = Set.empty[HasValue]): Future[ResultSet[JValue, CountType]] = 
    getHistogram(token, path, variable, tagTerms, additionalConstraints).map(v => v.toList.sortBy(_._2).take(n))

  /** Retrieves values of the specified variable.
   */
  def getValues(token: Token, path: Path, variable: Variable, tagTerms: Seq[TagTerm], additionalConstraints: Set[HasValue] = Set.empty[HasValue]): Future[Seq[JValue]] = 
    getHistogram(token, path, variable, tagTerms, additionalConstraints).map(_.map(_._1).toSeq)

  def getValuesTop(token: Token, path: Path, variable: Variable, n: Int, tagTerms: Seq[TagTerm], additionalConstraints: Set[HasValue] = Set.empty[HasValue]): Future[Seq[JValue]] = 
    getHistogramTop(token, path, variable, n, tagTerms, additionalConstraints).map(_.map(_._1))

  def getValuesBottom(token: Token, path: Path, variable: Variable, n: Int, tagTerms: Seq[TagTerm], additionalConstraints: Set[HasValue] = Set.empty[HasValue]): Future[Seq[JValue]] = 
    getHistogramBottom(token, path, variable, n, tagTerms, additionalConstraints).map(_.map(_._1))

  /** Retrieves the length of array properties, or 0 if the property is not an array.
   */
  def getVariableLength(token: Token, path: Path, variable: Variable): Future[Int] = {
    getVariableChildren(token, path, variable).map { hasChildren =>
      hasChildren.map(_._1.child.toString).filterNot(_.endsWith("/")).map(JPath(_)).foldLeft(0) {
        case (length, jpath) =>
          jpath.nodes match {
            case JPathIndex(index) :: Nil => (index + 1).max(length)
            case _ => length
          }
      }
    }
  }

  def getVariableStatistics(token: Token, path: Path, variable: Variable): Future[Statistics] = {
    getHistogram(token, path, variable, Nil).map { histogram => // TODO: Add term support
      (histogram.foldLeft(RunningStats.zero) {
        case (running, (hasValue, count)) =>
          val number = hasValue.value.deserialize[Double]

          running.update(number, count)
      }).statistics
    }
  }

  /** Retrieves a count of how many times the specified variable appeared in a path */
  def getVariableCount(token: Token, path: Path, variable: Variable, tagTerms: Seq[TagTerm]): Future[CountType] = {
    val fullFilterParts = List(Some(rawEventsBaseFilter(token, path)), 
                               rawEventsTagsFilter(tagTerms), 
                               Some(pathRollupFilter(path)),
                               eventNameFilter(variable),
                               variableExistsFilter(variable))

    val fullFilter = fullFilterParts.flatten.reduceLeft(_ & _)

    eventsdb(count.from(events_collection).where(fullFilter))
  }

  def aggregateVariableSeries(token: Token, path: Path, variable: Variable, encoding: TimeSeriesEncoding, period: Period, otherTerms: Seq[TagTerm], additionalConstraints: Set[HasValue]): Future[ValueStats] = {
    val emitter = """
  var v = this%1$s;
  if (typeof(v) == 'number' || v instanceof NumberLong) {
    var squared = v * v;
    emit("%2$d", { c : 1, s : v, q: squared });
  } else {
    emit("%2$d", { c : 1, s : NaN, q: NaN });
  }
""".format(JPath(".event.data") \ variable.name.tail, period.start.getMillis)

    val reduceFunc = """
function(key, values) {
  var count = 0;
  var sum = 0;
  var sumsq = 0;

  for (var i = 0; i < values.length; i++) {
    count += values[i].c;
    sum   += values[i].s;
    sumsq += values[i].q;
  }
  return { c : count, s : sum, q : sumsq };
}
"""

    queryMapReduce(token, path, variable, otherTerms ++ Seq(SpanTerm(encoding, period.timeSpan)), additionalConstraints, emitter, reduceFunc) {
      output => {
        eventsdb(selectOne().from(output.outputCollection)).map {
          result => {
            logger.trace("MR: result from " + output.outputCollection + " = " + result)
            result.map { jo => ValueStats(jvalToLong(jo("value")("c")), Some(jvalToDouble(jo("value")("s"))), Some(jvalToDouble(jo("value")("q")))) }.getOrElse { ValueStats(0,None,None) }
          }
        }
      }
    }
  }

  /** Retrieves a time series of statistics of occurrences of the specified variable in a path */
  def getVariableSeries(token: Token, path: Path, variable: Variable, tagTerms: Seq[TagTerm], additionalConstraints: Set[HasValue] = Set.empty): Future[ResultSet[JObject, ValueStats]] = {
    // Break out terms into timespan, named locations, and anonymous location prefixes (if any) 
    val (intervalOpt,locations,anonPrefixes) = tagTerms.foldLeft(Tuple3[Option[IntervalTerm], List[HierarchyLocationTerm], List[String]](None,Nil,Nil)) {
      case ((_,locs,prefixes),interval @ IntervalTerm(encoding, periodicity, TimeSpan(start,end))) => (Some(interval), locs, prefixes)
      case ((interval,locs,prefixes), loc @ HierarchyLocationTerm(name, Hierarchy.NamedLocation(_,_))) => (interval, loc :: locs, prefixes)
      case ((interval,locs,prefixes), loc @ HierarchyLocationTerm(name, Hierarchy.AnonLocation(path))) => (interval, locs, path.path :: prefixes)
      case (acc,_) => acc
    }

    val varPath = JPath(".event.data") \ variable.name.tail

    def aggregateEvents(allEvents: Iterator[JObject]) : ValueStats = {
      val startTime = System.currentTimeMillis
      var count = 0
      var sum = 0.0
      var sumsq = 0.0
      var numeric = false

      allEvents.foreach {
        event => {
          count = count + 1
          val eventValueStats = varPath.extract(event) match {
            case JInt(v)    => val vd = v.toDouble; numeric = true; sum = sum + vd; sumsq = sumsq + (vd * vd)
            case JDouble(d) => numeric = true; sum = sum + d; sumsq = sumsq + (d * d)
            case _          => // noop
          }
        }
      }

      val aggregated = if (numeric) ValueStats(count, Some(sum), Some(sumsq)) else ValueStats(count, None, None)
      logger.trace("Aggregated events to " + aggregated + " in " + (System.currentTimeMillis - startTime) + "ms")
      aggregated
    }

    val allObs = JointObservation(additionalConstraints + HasValue(variable, JNothing))

    logger.trace("Obtaining variable series on " + allObs)

    // Run the query over the interval given
    val results: Option[Future[ResultSet[JObject,ValueStats]]] = intervalOpt.map { interval => Future(interval.periods.map {
      period => {
        // If we have locations or prefixes, do location-based queries
        if (! (locations.isEmpty && anonPrefixes.isEmpty)) {
          // Named locations can be queried directly
          val namedLocResults: List[Future[List[(JObject,ValueStats)]]] = locations.map {
            case loc => {
              logger.trace("varseries with named location")
              aggregateVariableSeries(token, path, variable, interval.encoding, period, Seq(loc), additionalConstraints).map {
                vs => List((JObject(List(JField("timestamp", period.start.serialize), JField("location", loc.location.path))),
                            vs))
              }
            }
          }

          /* Because we can't filter on anonymous locations (they're free-form in the DB), we have to
           * just grab all events for the given interval and then group them app-side */
          val anonLocResults: Future[List[(JObject, ValueStats)]] = if (!anonPrefixes.isEmpty) {
            logger.trace("varseries with anon locations")
            // Make sure we retrieve location as part of the fields
            getRawEvents(token, path, allObs, Seq(SpanTerm(interval.encoding, period.timeSpan)), Right(List(JPath(".tags.#location")))).map {
              rawEvents => {
                // Compute statistics for any of the anon location prefixes that match our given events
                val prefixResults = rawEvents.foldLeft(Map.empty[String,List[JObject]]) {
                  case (acc, event) => locationValueMatch(anonPrefixes, event \ "tags" \ "#location") match {
                    case Some(matchedPrefix) => acc + (matchedPrefix -> acc.get(matchedPrefix).map(event :: _).getOrElse(List(event)))
                    case None                => acc
                  }
                }

                /* Now we determine stats for each of our prefixes based on the map. Non-existent prefixes
                 * Are set to zero so that we maintain a record for each period whether or not events match
                 * that period. */
                anonPrefixes.map {
                  prefix => {
                    val events = prefixResults.get(prefix).getOrElse(List())
                    val result = (JObject(List(JField("timestamp", period.start.serialize), JField("location", JString(prefix)))),
                                  aggregateEvents(events.iterator))
                    result
                  }
                }
              }
            }
          } else Future.sync(List[(JObject,ValueStats)]()) 

          Future((anonLocResults :: namedLocResults): _*).map(_.flatten)
        } else {
          // No location tags, so we'll just aggregate all events for this period
          logger.trace("varseries has no location, just timespan")
          aggregateVariableSeries(token, path, variable, interval.encoding, period, Seq(), additionalConstraints).map {
            vs => List((JObject(List(JField("timestamp", period.start.serialize))),vs))
          }
        }
      }
    }: _*).map{ _.flatten.sortBy(_._1)(JObjectOrdering) }}

    results.getOrElse(sys.error("Variable series requires a time span"))
  }

  /** Retrieves a count of the specified observed state over the given time period */
  def getObservationCount(token: Token, path: Path, observation: JointObservation[HasValue], tagTerms: Seq[TagTerm]): Future[CountType] = {
    getObservationSeries(token, path, observation, tagTerms) map (_.total)
  }

  /** Retrieves a time series of counts of the specified observed state
   *  over the given time period.
   */
  def getObservationSeries(token: Token, path: Path, observation: JointObservation[HasValue], tagTerms: Seq[TagTerm]): Future[ResultSet[JObject, CountType]] = {
    getRawEvents(token, path, observation, tagTerms).map(countByTerms(_, tagTerms).toSeq)
  }

  def getIntersectionCount(token: Token, path: Path, properties: List[VariableDescriptor], tagTerms: Seq[TagTerm], constraints: Set[HasValue]): Future[ResultSet[JArray, CountType]] = {
    getIntersectionSeries(token, path, properties, tagTerms, constraints) map {
      _.foldLeft(SortedMap.empty[JArray, CountType](JArrayOrdering)) {
        case (total, (key, timeSeries)) => 
          total + (key -> total.get(key).map(_ |+| timeSeries.total).getOrElse(timeSeries.total))
      }.toSeq
    } 
  }

  def getIntersectionSeries(token: Token, path: Path, variableDescriptors: List[VariableDescriptor], tagTerms: Seq[TagTerm], constraints: Set[HasValue]): Future[ResultSet[JArray, ResultSet[JObject, CountType]]] = {
    getRawIntersectionSeries(token, path, variableDescriptors, tagTerms, constraints)
  }

  private def getHistograms(token: Token, path: Path, variableDescriptors: List[VariableDescriptor]): Future[Map[VariableDescriptor, Map[JValue, CountType]]] = {
    Future {
      variableDescriptors map { descriptor =>
        val histogram = descriptor.sortOrder match {
          case SortOrder.Ascending  => getHistogramBottom _
          case SortOrder.Descending => getHistogramTop _
        }

        histogram(token, path, descriptor.variable, descriptor.maxResults, Nil, Set.empty[HasValue]).map(h => (descriptor, h.toMap))
      }: _*
    } map {
      _.toMap
    }
  }

  private def getRawIntersectionSeries(token: Token, path: Path, variableDescriptors: List[VariableDescriptor], tagTerms: Seq[TagTerm], constraints: Set[HasValue]): Future[ResultSet[JArray, ResultSet[JObject, CountType]]] = {
    if (variableDescriptors.isEmpty) Future.async(Nil) else {
      val eventVariables: Map[String, List[VariableDescriptor]] = variableDescriptors.groupBy(_.variable.name.head.collect{ case JPathField(name) => name }.get)

      // We'll limit our query to just the fields we need to obtain the intersection, plus the event name
      val observations = JointObservation(constraints.toSeq ++ variableDescriptors.map(vd => HasValue(vd.variable, JNothing)): _*)

      logger.trace("Querying mongo for intersection events with obs = " + observations)
      val queryStart = System.currentTimeMillis()

      getRawEvents(token, path, observations, tagTerms, Right(List(JPath(".event.name")))) map { events =>
          val eventsByName = events.groupBy(jv => (jv \ "event" \ "name").deserialize[String])

          logger.trace("Got %d results for intersection after %d ms".format(eventsByName.map(_._2.size).sum, System.currentTimeMillis() - queryStart))

          val resultMap = eventsByName.foldLeft(Map.empty[JArray, Map[JObject, CountType]]) {
            case (result, (eventName, namedEvents)) => 
              eventVariables.get(eventName) map { descriptors =>
                type Histograms = Map[VariableDescriptor, Map[JValue, Int]]

                /* Create histograms for the variables, discarding any events
                 * for which the variable value doesn't exist. */
                val allHistograms: Histograms = 
                  namedEvents.map((event: JValue) => variableDescriptors.flatMap(d => 
                    (event \ "event" \ "data").apply(d.variable.name.tail) match {
                      case JNothing => None
                      case goodVal => Some((d, Map(goodVal -> 1)))
                    })(collection.breakOut): Histograms).asMA.sum

                val toKeep: Map[VariableDescriptor, Set[JValue]] = allHistograms map {
                  case (v, m) => v.sortOrder match {
                    case SortOrder.Ascending  => (v -> m.toSeq.sortBy(_._2).take(v.maxResults).toMap.keySet)
                    case SortOrder.Descending => (v -> m.toSeq.sortBy(-_._2).take(v.maxResults).toMap.keySet)
                  }
                }

                val grouped: Map[Option[JArray], Iterable[JValue]] = namedEvents.groupBy { event =>
                  (descriptors map { d => (event \ "event" \ "data").apply(d.variable.name.tail).some.filter(toKeep(d)) }).sequence.map(JArray(_))
                } 
                
                grouped.foldLeft(result) { 
                  case (result, (Some(key), eventsForVariables)) => 
                    result + (key -> (result.getOrElse(key, Map.empty[JObject, CountType]) |+| countByTerms(eventsForVariables, tagTerms)))

                  case (result, _) => result
                }
              } getOrElse {
                result
              }
          }

          resultMap.mapValues(_.toSeq).toSeq
      }
    }
  }

  def findRelatedInfiniteValues(token: Token, path: Path, observation: JointObservation[HasValue], tagTerms: Seq[TagTerm]): Future[Iterable[HasValue]] = {
    def findInfiniteValues(fields: List[JField], path: JPath): List[HasValue] = fields.flatMap {
      case JField(name, value) if name startsWith "~" => List(HasValue(Variable(path \ name), value))
      case JField(name, JObject(fields)) => findInfiniteValues(fields, path \ name)
      case _ => Nil
    }

    getRawEvents(token, path, observation, tagTerms, Left(true)) map { 
      _.flatMap {
        case JObject(fields) => findInfiniteValues(fields, JPath.Identity)
        case _ => Nil
      }
    }
  }

  // the base filter is using a prefix-based match strategy to catch the path. This has the implication that paths are rolled up 
  // by default, which may not be exactly what we want. Hence, we later have to filter the returned results for only those where 
  // the rollup depth is greater than or equal to the difference in path length.
  private def rawEventsBaseFilter(token: Token, path: Path): MongoFilter = {
    JPath(".accountTokenId") === token.accountTokenId.serialize & JPath(".path").regex("^" + path.path)
  }

  private def pathRollupFilter(path: Path): MongoFilter = {
    MongoFilterBuilder(JPath(".min_ru")) <= path.length
  }

  // Some tags can be filtered. We can no longer pre-filter for location, since location is free-form in raw events
  private def rawEventsTagsFilter(tagTerms: Seq[TagTerm]): Option[MongoFilter] = {
    val tagsFilters: Seq[MongoFilter]  = tagTerms.collect {
      case IntervalTerm(_, _, span) => 
        (MongoFilterBuilder(JPath(".timestamp")) >= span.start.getMillis) & 
        (MongoFilterBuilder(JPath(".timestamp")) <  span.end.getMillis)

      case SpanTerm(_, span) =>
        (MongoFilterBuilder(JPath(".timestamp")) >= span.start.getMillis) & 
        (MongoFilterBuilder(JPath(".timestamp")) <  span.end.getMillis)

      case HierarchyLocationTerm(tagName, Hierarchy.NamedLocation(name, path)) =>
        MongoFilterBuilder(JPath(".tags") \ ("#" + tagName) \ name) === path.path
    }

    tagsFilters.reduceLeftOption(_ & _)
  }

  private def eventNameFilter(variable: Variable): Option[MongoFilter] =
    variable.name.head.flatMap {
      case JPathField(name) => Some(JPath(".event.name") === name)
      case _                => None
    }

  private def variableExistsFilter(variable: Variable): Option[MongoFilter] = 
    variable.name.tail.nodes match {
      case Nil => None
      case _   => Some((JPath(".event.data") \ variable.name.tail).isDefined)
    }

  private def constraintFilter(constraints : Set[HasValue]): Option[MongoFilter] = {
    val filters : Set[MongoFilter] = constraints.map {
      case HasValue(Variable(name), JNothing) => (JPath(".event.data") \ name.tail).isDefined
      case HasValue(Variable(name), value)    => (JPath(".event.data") \ name.tail) === value
    }

    filters.reduceLeftOption(_ & _)
  }

  private def hasAnonLocs(tagTerms: Seq[TagTerm]): Boolean = 
    tagTerms.contains { 
      v : Any => v match {
        case loc @ HierarchyLocationTerm(_, Hierarchy.AnonLocation(_)) => true
        case _ => false
      } 
    }

  private def rawEventsVariableFilter(token: Token, path: Path, variable: Variable, tagTerms: Seq[TagTerm]): MongoFilter = {
    List(Some(rawEventsBaseFilter(token, path)),
         rawEventsTagsFilter(tagTerms),
         eventNameFilter(variable),
         variableExistsFilter(variable)).flatten.reduceLeft(_ & _)
  }

  def getRawEvents(token: Token, path: Path, observation: JointObservation[HasValue], tagTerms: Seq[TagTerm], extraFields : Either[Boolean,List[JPath]] = Left(false)) : Future[MongoSelectQuery#QueryResult] = {
    // the base filter is using a prefix-based match strategy to catch the path. This has the implication that paths are rolled up 
    // by default, which may not be exactly what we want. Hence, we later have to filter the returned results for only those where 
    // the rollup depth is greater than or equal to the difference in path length.
    val baseFilter = rawEventsBaseFilter(token, path)

    // Some tags can be filtered. We can no longer pre-filter for location, since location is free-form in raw events
    val tagsFilter = rawEventsTagsFilter(tagTerms).map(baseFilter & _).getOrElse(baseFilter)

    val anonLocationPrefixes : Seq[String] = tagTerms.collect {
      case HierarchyLocationTerm(tagName, Hierarchy.AnonLocation(path)) => path.path
    }

    // We always need path and rollup and possibly need timestamp and/or location if we have proper tags
    val baseFieldsToRetrieve : List[JPath] = JPath(".path") :: JPath(".rollup") :: tagTerms.flatMap {
      case i : IntervalTerm          => Some(JPath(".timestamp"))
      case s : SpanTerm              => Some(JPath(".timestamp"))
      case l : HierarchyLocationTerm => Some(JPath(".tags.#location"))
      case _                         => None
    }.distinct.toList

    val (neededFields,filter) = observation.obs.foldLeft((baseFieldsToRetrieve, tagsFilter)) {
      case ((vars, acc), HasValue(variable, value)) => 
        val eventName = variable.name.head.flatMap {
          case JPathField(name) => Some(name)
          case _                => None
        }

        val (variableName,variableField) = variable.name.tail.nodes match {
          case Nil   => (None,None)
          // We can only consume up to the first indexed value (mongo is unhappy with selecting indexed fields)
          case nodes => (Some(variable.name.tail), Some(JPath(nodes.takeWhile(_.isInstanceOf[JPathField]))))
        }

        (variableField.map { vn => (JPath(".event.data") \ vn) :: vars }.getOrElse(vars), 
         acc &
         eventName.map(JPath(".event.name") === _) & 
         variableName.map { varTail => value match {
           case JNothing => (JPath(".event.data") \ (varTail)).isDefined   // JNothing means we just want to check existence
           case _        => ((JPath(".event.data") \ (varTail)) === value)
         }})
    }

    val fieldsToRetrieve = extraFields match {
      case Left(true)    => None // This means all fields
      case Left(false)   => Some(neededFields)
      case Right(extras) => Some((extras ::: neededFields).distinct)
    }

    retrieveEventsFor(filter, fieldsToRetrieve).map {
      _.filter { jv => 
        val eventPath = Path((jv \ "path").deserialize[String])
        //either the event path was equal to the path, or the difference between the length of the path (which must be longer)
        //and the queried path must be within range of the specified rollup
        val rollupOK = eventPath == path || (jv \? "rollup").flatMap(_.validated[Int].toOption).exists(r => (eventPath - path).map(_.length).exists(_ <= r))

        val locationOK = anonLocationPrefixes.size == 0 || locationValueMatch(anonLocationPrefixes, jv \ "tags" \ "#location").isDefined

        rollupOK && locationOK
      }
    }
  }

  // The location of the event must either match the anonLocationPrefixes, or contain a field whose value matches
  def locationValueMatch(prefixes: Seq[String], locVal : JValue) : Option[String] = {
    locVal match {
      case JString(eventLoc)       => prefixes.find(eventLoc.startsWith)
      case JField(_, value)        => locationValueMatch(prefixes, value)
      case JArray(values)          => values.flatMap(locationValueMatch(prefixes,_)).headOption
      case JObject(eventLocFields) => eventLocFields.flatMap(locationValueMatch(prefixes,_)).headOption
      case _                       => None
    }   
  }


  private def searchQueriableExpansion[T](span: TimeSpan, f: (IntervalTerm) => Future[T]): Future[List[T]] = {
    import TimeSpan._
    val futureResults = timeSeriesEncoding.queriableExpansion(span) map {
      case (p, span) => f(IntervalTerm(timeSeriesEncoding, p, span))
    }

    Future(futureResults: _*)
  }

  private def internalSearchSeries[T: AbelianGroup : Extractor](tagTerms: Seq[TagTerm], filter: Sig => MongoFilter, dataPath: JPath, collection: MongoCollection): Future[ResultSet[JObject, T]] = {

    val toQuery = tagTerms.map(_.storageKeys).filter(!_.isEmpty).sequence.map {
      v => Tuple2(
        v.map(_._1).toSet.sig,
        v.map(_._2).sequence.map(x => (x.map(_._1).toSet.sig, JObject(x.map(_._2).toList)))
      )
    } 

    Future (
      toQuery.map {
        case (docKey, dataKeys) => indexdb(selectOne(dataPath).from(collection).where(filter(docKey))).map {
          result => dataKeys.map {
            case (keySig, keyData) => 
               // since the data keys are a stream of all possible keys in the document that data might
               // be found under, and since we want to return a zero-filled result set, we use the \? form of
               // the object find method and then use getOrElse returning the zero for T if no such key is found.
               keyData -> (result flatMap { jobj => (jobj(dataPath) \? keySig.hashSignature).map(_.deserialize[T]) } getOrElse mzero[T])
          }
        }
      }.toSeq: _*
    ) map {
      _.flatten
    }
  }

  private def extractChildren[T](filter: MongoFilter, collection: MongoCollection)(extractor: (JValue, CountType) => T): Future[List[T]] = {
    indexdb {
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
    indexdb {
      selectOne(".values", ".indices").from(collection).where(filter)
    } map { 
      case None => logger.trace("extractValues got nothing"); Nil

      case Some(result) =>
        logger.trace("extractValues got result: " + result)
        val retVal = ((result \ "indices") match {
          case JObject(fields) => 
            fields.flatMap{ 
              case JField(name, JInt(count)) => Some(extractor(JInt(name.toInt), count.toLong))
              case badness => logger.warn("Non-index field found in indices: " + badness); None
            }
          case _ => Nil
        }) ++ 
        (result \ "values").children.collect {
          case JField(name, count) =>
            val jvalue = JsonParser.parse(MongoEscaper.decode(name))

            extractor(jvalue, count.deserialize[CountType])
        }

        logger.trace("extractValues transformed to " + retVal)
        retVal
    }
  }

  private def forTokenAndPath(token: Token, path: Path): MongoFilter = {
    (".accountTokenId" === token.accountTokenId) &
    (".path"           === path.toString)
  }

  private def forVariable(variable: Variable): MongoFilter = {
    ".variable" === variable.serialize
  }

  private def forHierarchyTag(token: Token, path: Path, tagName: String) = {
    forTokenAndPath(token, path) & (".tagName" === tagName)
  }

  /*********************
   * UPDATE GENERATION *
   ******************** */

  /** Creates a bunch of patches to keep track of parent/child path relationships.
   * E.g. if you send "/foo/bar/baz", it will keep track of the following:
   *
   * "/foo" has child "bar"
   * "/foo/bar" has child "baz"
   */
  private def addPathChildren(token: Token, path: Path): MongoPatches = {
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

  private def addPathTags(token: Token, path: Path, tags: Seq[Tag]): (MongoFilter, MongoUpdate) = {
    forTokenAndPath(token, path) -> tags.foldLeft(MongoUpdate.Empty) {
      case (update, Tag(name, _)) => update |+| (JPath(".tags") \ JPathField(MongoEscaper.encode(name)) inc 1)
    }
  }

  /**
   * Increments the count of each location within hierarchy path tags in the tag set
   */
  private def addHierarchyChildren(token: Token, path: Path, tags: Seq[Tag]): MongoPatches = {
    tags.foldLeft(MongoPatches.empty) {
      case (patches, Tag(name, NameSet(values))) => 
        val update = values.foldLeft(MongoUpdate.Empty) {
          case (update, value) if !value.trim.isEmpty => 
            update |+| ((JPath(".values") \ JPathField(MongoEscaper.encode(value))) \ JPathField("#count") inc 1)

          case (update, _) => update 
        }

        patches + (forHierarchyTag(token, path, name) -> update)

      case (patches, Tag(name, Hierarchy(locations))) =>
        val update = locations.foldLeft(MongoUpdate.Empty) {
          (update, location) => 
            update |+| (JPath(".values") \ JPath(location.path.elements.map((JPathField.apply _) compose (MongoEscaper.encode _)): _*) \ JPathField("#count") inc 1)
        }

        patches + (forHierarchyTag(token, path, name) -> update)

      case (patches, _) => patches
    }
  }

  /** Creates patches to record variable observations.
   */
  private def variableChildrenPatches(token: Token, path: Path, observations: Set[HasChild], count: CountType): MongoPatches = {
    observations.foldLeft(MongoPatches.empty) { 
      case (patches, HasChild(variable, childNode)) =>
        val filterVariable = forTokenAndPath(token, path) & forVariable(variable)

        val update = childNode match {
          case JPathField(name) => 
            // Need to quote the field name so that it deserializes to a string later
            (JPath(".values") \ JPathField("\"" + name + "\"")) inc count

          case index            => 
            (JPath(".indices") \ index) inc count
        }

        patches + (filterVariable -> update)
    }
  }

  def docKeySig(storageKeys: Seq[StorageKeys]): Sig = {
    val keySigs = storageKeys.map {
      // this redundancy is necessary to get the correct implicits used for sig generation
      case NameSetKeys(docKey, _)   => docKey.sig
      case TimeRefKeys(docKey, _)   => docKey.sig
      case HierarchyKeys(docKey, _) => docKey.sig
    }

    keySigs.toSet.sig
  }

  /**
   * The first element of the resulting tuple is the key that observations correlated to the tags from which
   * the specified set of storage keys was derived will be stored against.
   * The second element of the resulting tuple is the "reference" signature used to relate the
   * observations to associated infinite values.
   */
  def dataKeySigs(storageKeys: Seq[StorageKeys]): (Sig, Sig) = {
    storageKeys.map {
      case NameSetKeys(_, dataKey)      => (None, dataKey.sig)
      case TimeRefKeys(docKey, instant) => (Some(instant.sig), Sig(docKey._1.sig, instant.sig))
      case HierarchyKeys(_, dataKey)    => (Some(dataKey.sig), dataKey.sig)
    }.foldLeft((Set.empty[Sig], Set.empty[Sig])) {
      case ((ks1, ks2), (k1, k2)) => (ks1 ++ k1, ks2 + k2)
    } mapElements (
      _.sig,
      _.sig
    )
  }

  def stop(timeout: akka.actor.Actor.Timeout) = {
    val stageStops = variable_children.stage.stop ::
                     path_children.stage.stop :: 
                     path_tags.stage.stop :: 
                     hierarchy_children.stage.stop :: Nil

    akka.dispatch.Future.sequence(stageStops, timeout.duration.toMillis)
  }
}

object AggregationEngine {
  type CountType = Long
  type ResultSet[K <: JValue, V] = Seq[(K, V)]

  import blueeyes.bkka.Stop
  implicit def stop: Stop[AggregationEngine] = new Stop[AggregationEngine] {
    def stop(engine: AggregationEngine) = engine.stop(akka.actor.Actor.Timeout(Long.MaxValue))
  }

  implicit def rsRich[K <: JValue, V: AbelianGroup](resultSet: ResultSet[K, V]): RichResultSet[K, V] = new RichResultSet(resultSet)
  class RichResultSet[K <: JValue, V: AbelianGroup](resultSet: ResultSet[K, V]) {
    def total: V = resultSet.map(_._2).asMA.sum
    def mapValues[X](f: V => X): ResultSet[K, X] = resultSet.map(f.second)
  }

  private val seriesId = "id"
  private val valuesId = "id"

  private val DataPath = JPath(".data")
  private val CountsPath = JPath(".counts")

  val timeSeriesEncoding  = TimeSeriesEncoding.Default
  val timeGranularity     = timeSeriesEncoding.grouping.keys.min

  private val EventsdbIndices = Map(
    "events" -> Map(
      "raw_events_query" -> (List("accountTokenId", "path", "timestamp"), false)
    )
  )

  private val IndexdbIndices = Map(
    "path_children" -> Map(
      "path_child_query"  -> (List("accountTokenId", "path", "child"), false)
    ),
    "variable_children" -> Map(
      "variable_query"    -> (List("accountTokenId", "path", "variable"), false)
    )
  )

  private def createIndices(database: Database, toCreate: Map[String, Map[String, (List[String], Boolean)]]) = {
    val futures = for ((collection, indices) <- toCreate; 
                       (indexName, (fields, unique)) <- indices) yield {
      database {
        if (unique) ensureUniqueIndex(indexName + "_index").on(fields.map(JPath(_)): _*).in(collection)
        else ensureIndex(indexName + "_index").on(fields.map(JPath(_)): _*).in(collection)
      }.toUnit
    }

    Future(futures.toSeq: _*)
  }

  def apply(config: ConfigMap, logger: Logger, eventsdb: Database, indexdb: Database, healthMonitor: HealthMonitor)(implicit hashFunction: HashFunction = Sha1HashFunction): Future[AggregationEngine] = {
    for {
      _ <- createIndices(eventsdb, EventsdbIndices)
      _ <- createIndices(indexdb,  IndexdbIndices)
    } yield {
      new AggregationEngine(config, logger, eventsdb, indexdb, ClockSystem.realtimeClock, healthMonitor)
    }
  }

  def forConsole(config: ConfigMap, logger: Logger, eventsdb: Database, indexdb: Database, healthMonitor: HealthMonitor)(implicit hashFunction: HashFunction = Sha1HashFunction) = {
    new AggregationEngine(config, logger, eventsdb, indexdb, ClockSystem.realtimeClock, healthMonitor)
  }

  def intersectionOrder[T <% Ordered[T]](histograms: List[(SortOrder, Map[JValue, T])]): scala.math.Ordering[JArray] = {
    new scala.math.Ordering[JArray] {
      override def compare(l1: JArray, l2: JArray) = {
        val valuesOrder = (l1.elements zip l2.elements).zipWithIndex.foldLeft(0) {
          case (0, ((v1, v2), i)) => 
            val (sortOrder, histogram) = histograms(i)  
            val valueOrder = histogram(v1) compare histogram(v2)
            sortOrder match {
              case SortOrder.Ascending  => -valueOrder
              case SortOrder.Descending => valueOrder
            }
           
          case (x, _) => x
        }

        if (valuesOrder == 0) JArrayOrdering.compare(l1, l2) else valuesOrder
      }
    }   
  }

  def countByTerms(results: Iterable[JValue], tagTerms: Seq[TagTerm]): Map[JObject, CountType] = {
    val retrieved = results.foldLeft(SortedMap.empty[JObject, CountType](JObjectOrdering)) { (acc, event) =>
      val key = JObject(
        tagTerms.collect {
          case IntervalTerm(_, periodicity, _) => JField("timestamp", periodicity.period(event \ "timestamp").start)
          case HierarchyLocationTerm(tagName, Hierarchy.AnonLocation(path)) => JField(tagName, path.path)
          case HierarchyLocationTerm(tagName, Hierarchy.NamedLocation(name, path)) => JField(tagName, path.path)
        }.toList
      )

      acc + (key -> (acc.getOrElse(key, 0L) + ((event \ "count").validated[Long] | 1L)))
    }

    val intervalPeriods = tagTerms.collect {
      case IntervalTerm(_, periodicity, span) => periodicity.period(span.start).until(span.end)
      case _ => Stream.empty[Period]
    }

    intervalPeriods.toStream.flatten.foldLeft(retrieved) {
      case (acc, period) =>
        val key = JObject(
          JField("timestamp", period.start) ::
          tagTerms.collect {
            case HierarchyLocationTerm(tagName, Hierarchy.AnonLocation(path)) => JField(tagName, path.path)
            case HierarchyLocationTerm(tagName, Hierarchy.NamedLocation(name, path)) => JField(tagName, path.path)
          }.toList
        )
        
        if (acc.contains(key)) acc else acc + (key -> 0L)
    }
  }
}
