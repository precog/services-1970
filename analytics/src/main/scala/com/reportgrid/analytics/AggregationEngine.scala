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

class AggregationEngine private (config: ConfigMap, val logger: Logger, val insertEventsdb: Database, val queryEventsdb: Database, val queryIndexdb: Database, clock: Clock, val healthMonitor: HealthMonitor)(implicit hashFunction: HashFunction = Sha1HashFunction) {
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
        database   = queryIndexdb,
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

  final val anonLocMatchFunc = """
  function anonLocMatch(obj, prefixes) {
      if (typeof obj == 'string') {
          for (var i in prefixes) {
              var prefix = prefixes[i];
              if (obj.substring(0, prefix.length) == prefix) {
                  return prefix;
              }
          }

          return null;
      }
      if (typeof obj == 'object') {
          for (var prop in obj) {
              return anonLocMatch(obj[prop], prefixes)
          }
      }
      return null;
  }

"""

  private val variable_children         = AggregationStage("variable_children")
  private val path_children             = AggregationStage("path_children")

  private val path_tags                 = AggregationStage("path_tags")
  private val hierarchy_children        = AggregationStage("hierarchy_children")

  def flushStages = List(variable_children, path_children).map(_.stage.flushAll).sequence.map(_.sum)

  val events_collection: MongoCollection = config.getString("events.collection", "events")

  val stats_cache_collection: MongoCollection = config.getString("stats_cache.collection", "stats_cache")

  val histo_cache_collection: MongoCollection = config.getString("histo_cache.collection", "histo_cache")

  def store(token: Token, path: Path, eventName: String, eventBody: JValue, tagResults: Tag.ExtractionResult, count: Int, rollup: Int, reprocess: Boolean) = {
    logger.trace("Storing event: " + eventBody + " at " + path + " token " + token)
    if (token.limits.lossless) {
      def withTagResults[A](f: Seq[Tag] => Future[A]): Future[A] = tagResults match {
        case Tag.Tags(tf) => tf.flatMap(f)
        case _ => f(Nil)
      }

      withTagResults { (tags: Seq[Tag]) =>
        logger.trace("  with tags:" + tags)
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

        insertEventsdb(MongoInsertQuery(events_collection, List(record))) 
      } 
    } else {
      Future.sync(())
    }
  }

  def retrieveEventsFor(filter : MongoFilter, fields : Option[List[JPath]] = None) : Future[scala.collection.IterableView[JObject, Iterator[JObject]]] = {
    logger.trace("Fetching events for filter " + compact(render(filter.filter)) + ", with fields: " + fields.map(_.mkString(",")).getOrElse("all"))
    fields.map {
      fieldsToGet => queryEventsdb(select(fieldsToGet : _*).from(events_collection).where(filter))
    }.getOrElse(queryEventsdb(selectAll.from(events_collection).where(filter))).map {
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
          case invalid      => logger.error("Invalid var child value: " + invalid); sys.error("Invalid variable child result")
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
      queryEventsdb(selectAll.from(output.outputCollection)).map { 
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
    queryIndexdb(selectAll.from(path_tags.collection).where(forTokenAndPath(token, path))) map { results =>
      val res = results.map(_ \ "tags") flatMap {
        case r @ JObject(fields) => fields.map(_.name)
        case _ => Nil
      }
      
      res.toList
    }
  }

  def getHierarchyChildren(token: Token, path: Path, tagName: String, jpath: JPath) = {
    val dataPath = JPath(".values") \ jpath
    queryIndexdb(select(dataPath).from(hierarchy_children.collection).where(forHierarchyTag(token, path, tagName))) map { results =>
      results.map(_(dataPath)).flatMap {
        case JObject(fields) => fields.map(_.name).filter(_ != "#count")
        case _ => Nil
      }
    }
  }

  // Cannot be used for queries with anon locs
  private def queryAggregation[T](token: Token, path: Path, variable: Variable, tagTerms: Seq[TagTerm], where: Set[HasValue], pipeline: List[JObject])(finalizer: Option[JObject] => Future[T]): Future[T] = {
    val filter = fullFilter(token, path, variable, tagTerms, where)

    val matchStep = JObject(List(JField("$match", filter.filter)))

    // Just in case the value is an array, we try to unwind first and then check to see if the result is an error
    val unwindPipe = JObject(List(JField("$unwind", "$" + (JPath(".event.data") \ variable.name.tail).toString.drop(1))))

    val finalPipe = JArray(matchStep :: unwindPipe :: pipeline)

    logger.trace("Aggregation: Final pipe = " + pretty(render(finalPipe)))

    queryEventsdb(aggregation(finalPipe).from(events_collection)).flatMap {
      firstResult => {
        logger.trace("Aggregation: received unwound result: " + firstResult)
        val unwindInvalid = firstResult match {
          case Some(obj) => (obj("ok") == JInt(0) || obj("ok") == JDouble(0)) // && obj("code") == JInt(15978)
          case _         => false
        }
        
        if (unwindInvalid) {
          logger.trace("Unwind invalid, re-running without unwind")
          queryEventsdb(aggregation(JArray(matchStep :: pipeline)).from(events_collection)).flatMap(finalizer)
        } else {
          finalizer(firstResult)
        }
      }
    }
  }

  private def queryMapReduce[T](token: Token, path: Path, variable: Variable, tagTerms: Seq[TagTerm], where: Set[HasValue], emitter: String, reducer: String)(finalizer: MapReduceOutput => Future[T]): Future[T] = {
    val filter = fullFilter(token, path, variable, tagTerms, where)

    // Results go to temp collection. This works around a MongoDB Java driver bug as well as allowing for result sets > 16MB
    val resultCollection = ("mapreduce" + java.util.UUID.randomUUID.toString).replace("-","")

    logger.trace("MR: Sending results to " + resultCollection)

    val mapFunc = """
function() {
  if(!Array.isArray) {  
    Array.isArray = function (arg) {  
      return Object.prototype.toString.call(arg) == '[object Array]';  
    };  
  }  

  %s;
}
""".format(emitter)

    logger.trace("MR: MapFunc = " + mapFunc)
    logger.trace("MR: Reducer = " + reducer)
    logger.trace("MR: Filter " + compact(render(filter.filter)))
    
    queryEventsdb(mapReduce(mapFunc, reducer).from(events_collection).where(filter).into(resultCollection)).flatMap {
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

  def getHistogram(token: Token, path: Path, variable: Variable, tagTerms : Seq[TagTerm], additionalConstraints: Set[HasValue] = Set.empty[HasValue]): Future[Map[JValue, CountType]] = {
    val isArrayVar = variable.name.nodes.lastOption match {
      case Some(JPathIndex(_)) => true
      case _ => false
    }

    val isAnonLoc = hasAnonLocs(tagTerms)

    val nonTimeTags = tagTerms.filterNot {
      case s : SpanTerm     => true
      case i : IntervalTerm => true
      case _                => false
    }.toList

    val filter = fullFilter(token, path, variable, tagTerms, additionalConstraints)

    val whereClause = cacheWhereClauseFor(nonTimeTags, variable, additionalConstraints)

    rangeFor(tagTerms, filter).flatMap {
      case Some((start,end)) => {
        logger.trace("Histogram over %s -> %s for ".format(start, end))

        // determine the period boundary start and end from the given range. These may not be the same if start/end is within a period
        val hourStart = Periodicity.Hour.increment(Periodicity.Hour.floor(start))
        val hourEnd   = Periodicity.Hour.floor(end)

        // First find any cached entries that we can use
        queryIndexdb(select(JPath(".timestamp"), JPath(".histogram")).from(histo_cache_collection).where(cacheFilterFor(token, path, whereClause, tagTerms, start.getMillis, end.getMillis, false))) flatMap {
          found => {
            val (foundBuckets: List[(JValue,Long)], foundPeriods) = found.foldLeft((List[(JValue,Long)](), Set[Period]())) {
              case ((buckets,periods), entry) => {
                val entryBuckets = (entry \ "histogram") match {
                  case JArray(histoEntries) => histoEntries.map {
                    obj => (obj \ "label", (obj \ "count").deserialize[Long])
                  }
                  case invalid => logger.error("Invalid histo result: " + invalid); sys.error("Invalid histogram result")
                }
                (entryBuckets ::: buckets, periods + Period(Periodicity.Hour, (entry \ "timestamp").deserialize[Instant]))
              }
            }

            val allPeriods = Period(Periodicity.Hour, hourStart).until(hourEnd).toSet
            
            val queryPeriods = allPeriods -- foundPeriods

            if (queryPeriods.size < 4000) {
              logger.trace("Found %d cached periods, querying for %d".format(foundPeriods.size, queryPeriods.size))
              val queryResults: List[Future[List[(JValue,Long)]]] = queryPeriods.toList.map {
                period => runHistogramQuery(token, path, variable, SpanTerm(timeSeriesEncoding, period.timeSpan) :: nonTimeTags, additionalConstraints, isArrayVar || isAnonLoc).map {
                  resultMap => {
                    val resultList = resultMap.toList
                    val toSave = JObject(List(JField("accountTokenId", token.accountTokenId.serialize),
                                              JField("path", path.serialize),
                                              JField("timestamp", period.start.serialize),
                                              JField("where", whereClause.serialize),
                                              JField("histogram", JArray(resultList.map {
                                                case (label,count) => JObject(JField("label", label) :: JField("count", count.serialize) :: Nil)
                                              }))))

                    queryIndexdb(upsert(histo_cache_collection).set(toSave).where(JPath(".accountTokenId") === token.accountTokenId.serialize & JPath(".path") === path.path & (timestampField === period.start.getMillis) & JPath(".where") === whereClause))
                    resultList
                  }
                }
              }

              val prePeriodResults: Future[List[(JValue,Long)]] = runHistogramQuery(token, path, variable, SpanTerm(timeSeriesEncoding, TimeSpan(start, hourStart)) :: nonTimeTags, additionalConstraints, isArrayVar || isAnonLoc).map(_.toList)

              val postPeriodResults: Future[List[(JValue,Long)]] = runHistogramQuery(token, path, variable, SpanTerm(timeSeriesEncoding, TimeSpan(hourEnd, end)) :: nonTimeTags, additionalConstraints, isArrayVar || isAnonLoc).map(_.toList)

              Future((prePeriodResults :: postPeriodResults :: queryResults): _*).map {
                periodLists: List[List[(JValue,Long)]] => {
                  val allResults: List[(JValue,Long)] = (foundBuckets :: periodLists).flatten
                  allResults.groupBy(_._1).map {
                    case (bucket, counts) => (bucket, counts.map(_._2).sum)
                  }
                }
              }
            } else {
              logger.warn("Running uncached histogram on oversized query (%d periods needed)".format(queryPeriods.size))
              runHistogramQuery(token, path, variable, tagTerms, additionalConstraints, isArrayVar || isAnonLoc)
            }
          }
        }
      }.map { results => logger.debug("Final histo result for %s = %s".format(compact(render(filter.filter)), results)); results }
      case _ => Future.sync(Map()) // No range means no entries
    }
  }

  private def runHistogramQuery (token: Token, path: Path, variable: Variable, tagTerms: Seq[TagTerm], additionalConstraints: Set[HasValue], useMR: Boolean): Future[Map[JValue, CountType]] = {
    if (useMR) {
      logger.trace("Forcing MR on histogram over array variable")
      getMRHistogram(token, path, variable, tagTerms, additionalConstraints)
    } else {
      val variableName = (JPath(".event.data") \ variable.name.tail).toString.drop(1) // Drop leading dot

      val pipeline = List(JObject(List(JField("$project", JObject(List(JField(variableName, JInt(1))))),
                                       JField("$group", JObject(List(JField("_id", "$" + variableName),
                                                                     JField("count", JObject(List(JField("$sum", JInt(1)))))))))))

      queryAggregation(token, path, variable, tagTerms, additionalConstraints, pipeline)  {
        case Some(result) => result("result") match {
          case JArray(objs) => {
            logger.trace("Aggregation results: " + objs)
            Future.sync(objs.toList.flatMap {
              jo => {
                val key = jo("_id")
                val count: Long = jo("count").deserialize[Long]
                if (key == JNull) None else Some((key,count)) // We don't report Null values
              }
            }.toMap)
          }
          case invalid => logger.error("Invalid histogram result: " + result); sys.error("Invalid histogram result")
        }
        case _ => Future.sync(Map())
      }
    }
  }


  def getMRHistogram(token: Token, path: Path, variable: Variable, tagTerms : Seq[TagTerm], additionalConstraints: Set[HasValue] = Set.empty[HasValue]): Future[Map[JValue, CountType]] = {
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
      queryEventsdb(selectAll.from(output.outputCollection)).map { 
        objs => {
          val resultMap = objs.toList.map {
            jo => {
              val key = jo("_id")
              val count: Long = jo("value").deserialize[Long]
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
    val filter = fullFilter(token, path, variable, tagTerms, Set())

    logger.debug("Variable count on " + filter.filter)

    val nonTimeTags = tagTerms.filterNot {
      case s : SpanTerm     => true
      case i : IntervalTerm => true
      case _                => false
    }.toList

    val untimedFilter = fullFilter(token, path, variable, nonTimeTags, Set())

    val now = new Instant

    rangeFor(tagTerms, filter).flatMap {
      case Some((start,end)) if start isBefore now => {
        logger.trace("Series count for %s -> %s".format(start, end))

        // determine the period boundary start and end from the given range. These may not be the same if start/end is within a period
        val hourStart = Periodicity.Hour.increment(Periodicity.Hour.floor(start))
        val hourEnd   = Periodicity.Hour.floor(end)

        // Delegate to a series query to handle caching, etc
        val newTerms = IntervalTerm(timeSeriesEncoding, Periodicity.Single, TimeSpan(hourStart, hourEnd)) :: nonTimeTags
        
        val mainCounts = List(getVariableSeries(token, path, variable, newTerms).map { _.toList.drop(1).map { _._2.count }.sum })

        // We also need to cover any non-integral periods at the beginning and end of our ranges
        val startPortionCount = if (start isBefore hourStart) {
          logger.trace("Querying for pre-period portion")
          queryEventsdb(count.from(events_collection).where(untimedFilter & (timestampField >= start.getMillis) & (timestampField < hourStart.getMillis))).map {
            v => logger.trace("Pre-period = " + v); v
          }
        } else {
          Future.sync(0l)
        }

        val endPortionCount = if (end isAfter hourEnd) {
          logger.trace("Querying for post-period portion")
          queryEventsdb(count.from(events_collection).where(untimedFilter & (timestampField >= hourEnd.getMillis) & (timestampField < end.getMillis))).map {
            v => logger.trace("Post-period = " + v); v
          }
        } else {
          Future.sync(0l) 
        }

        Future((startPortionCount :: endPortionCount :: mainCounts): _*).map {
          counts => val finalSum = counts.sum; logger.debug("Final count = " + finalSum + " for " + compact(render(filter.filter))); finalSum
        }
      }
      case Some(_) => logger.debug("Simple count on future/large results"); queryEventsdb(count.from(events_collection).where(filter))
      case None => logger.debug("Empty range on variable count, returning zero"); Future.sync(0l) // If we couldn't find a range it's because nothing exists
    }
  }

  def aggregateVariableSeries(token: Token, path: Path, variable: Variable, encoding: TimeSeriesEncoding, period: Period, otherTerms: Seq[TagTerm], additionalConstraints: Set[HasValue]): Future[ValueStats] = {
    logger.trace("Aggregate variable series on " + variable)
    val variableName = (JPath(".event.data") \ variable.name.tail).toString.drop(1) // Drop leading dot

    val pipeline = List(JObject(List(JField("$project", JObject(List(JField("val", JString("$" + variableName)),
                                                                     JField("squared", JObject(List(JField("$multiply", JArray(List(JString("$" + variableName), JString("$" + variableName)))))))))))),
                        JObject(List(JField("$group", JObject(List(JField("_id", JInt(period.start.getMillis)),
                                                                   JField("count", JObject(List(JField("$sum", JInt(1))))),
                                                                   JField("sum", JObject(List(JField("$sum", JString("$val"))))),
                                                                   JField("sumsq", JObject(List(JField("$sum", JString("$squared")))))))))))
    
    val fullTerms = otherTerms ++ Seq(SpanTerm(encoding, period.timeSpan))
    
    queryAggregation(token, path, variable, fullTerms, additionalConstraints, pipeline)  {
      case Some(firstResult) => {
        val nonNumeric = firstResult match {
          case obj if (obj("ok") == JInt(0) || obj("ok") == JDouble(0)) => true // && obj("code") == JInt(16005) => true
          case _         => false
        }
        
        if (nonNumeric) {
          // If it's non-numeric all we can do is count it
          logger.trace("Non-numeric varseries. Counting")
          queryEventsdb(count.from(events_collection).where(fullFilter(token, path, variable, fullTerms, additionalConstraints))).map { count => ValueStats(count, None, None) }
        } else {
          firstResult("result") match {
            case JArray(objs) => {
              logger.trace("Aggregation results: " + objs)
              Future.sync(objs.toList.map {
                jo => {
                  val key = jo("_id")
                  val count: Long = jo("count").deserialize[Long]
                  val sum = Some(jo("sum").deserialize[Double])
                  val sumsq = Some(jo("sumsq").deserialize[Double])

                  ValueStats(count, sum, sumsq)
                }
              }.headOption.getOrElse(ValueStats(0,None,None)))
            }
            case invalid => logger.error("Invalid var series result: " + invalid); sys.error("Invalid variable series result")
          }
        }
      }
      case None => Future.sync(ValueStats(0, None, None))
    }
  }

  // This method is only used when we have anonymous location tags
  def mapReduceVariableSeries(token: Token, path: Path, variable: Variable, encoding: TimeSeriesEncoding, period: Period, additionalConstraints: Set[HasValue], prefixes : Seq[String]): Future[ResultSet[JObject, ValueStats]] = {
    val emitter = anonLocMatchFunc + """
  function emitByType(v, p) {
      if (typeof(v) == 'number' || v instanceof NumberLong) {
          var squared = v * v;
          emit({ "timestamp" : %1$d, "location" : p }, { c : 1, s : v, q: squared });
      } else {
          emit({ "timestamp" : %1$d, "location" : p }, { c : 1, s : NaN, q: NaN });
      }
  }

  var matchedPrefix = anonLocMatch(this['tags']['#location'], %2$s);

  if (matchedPrefix != null) {
      var testVal = this%3$s;

      if (Array.isArray(testVal)) {
          for (var index in testVal) {
              if (testVal[index] != null) {
                  emitByType(testVal[index], matchedPrefix);
              }
          }
      } else {
          if (testVal != null) {
              emitByType(testVal, matchedPrefix);
          }
      }
  }
    """.format(period.start.getMillis, prefixes.mkString("[\"", "\",\"", "\"]"), JPath(".event.data") \ variable.name.tail)

    val reducer = """
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

    logger.trace("MR varseries emitter: " + emitter)
    logger.trace("MR varseries reducer: " + reducer)

    queryMapReduce(token, path, variable, Seq(SpanTerm(encoding, period.timeSpan)), additionalConstraints, emitter, reducer) {
      output => {
        queryEventsdb(selectAll.from(output.outputCollection)).map {
          result => {
            val resList = result.toList
            logger.trace("MR varseries results from " + output.outputCollection + ": " + resList)
            resList.map { jo => (jo("_id").asInstanceOf[JObject], ValueStats((jo \ "value" \  "c").deserialize[Long], Some((jo \ "value" \ "s").deserialize[Double]), Some((jo \ "value" \ "q").deserialize[Double]))) } match {
              case Nil => List((JObject(JField("timestamp", JInt(period.start.getMillis)) :: Nil), ValueStats(0, None, None)))
              case r   => r
            }
          }
        }
      }
    }
  }

  /** Retrieves a time series of statistics of occurrences of the specified variable in a path */
  def getVariableSeries(token: Token, path: Path, variable: Variable, tagTerms: Seq[TagTerm], additionalConstraints: Set[HasValue] = Set.empty): Future[ResultSet[JObject, ValueStats]] = {
    // Break out terms into timespan, named locations, and anonymous location prefixes (if any) 
    val intervalOpt  = tagTerms.collectFirst { case i : IntervalTerm => i }
    val locations    = tagTerms.collect { case loc @ HierarchyLocationTerm(name, Hierarchy.NamedLocation(_,_)) => loc }.toList
    val anonPrefixes = tagTerms.collect { case loc @ HierarchyLocationTerm(name, Hierarchy.AnonLocation(path)) => path.path }

    val allObs = JointObservation(additionalConstraints + HasValue(variable, JNothing))

    logger.debug("Obtaining variable series on " + allObs + " by " + intervalOpt)

    val locationClause = locationClauseFor(tagTerms)

    val now = new Instant

    // Run the query over the intervals given
    val results: Option[Future[ResultSet[JObject,ValueStats]]] = intervalOpt.map { interval => Future(interval.periods.map {
      outerPeriod => {
        logger.trace("Querying for period: " + outerPeriod)

        // For this period, find any cached counts (hourly) within the period, query for any missing, then sum and return the final count
        val whereClause = cacheWhereClauseFor(tagTerms, variable, additionalConstraints)
        val cacheFilter = cacheFilterFor(token, path, whereClause, tagTerms, outerPeriod.start.getMillis, outerPeriod.end.getMillis, true)

        queryIndexdb(select(JPath(".timestamp"), JPath(".location"), JPath(".count"), JPath(".sum"), JPath(".sumsq")).from(stats_cache_collection).where(cacheFilter)).flatMap {
          found => {
            val (outOfRange,neededPeriods) = if (interval.resultGranularity == Periodicity.Hour || interval.resultGranularity == Periodicity.Day || interval.resultGranularity == Periodicity.Single) {
              logger.trace(interval.resultGranularity.name + " series in range")
              (false,Period(Periodicity.Hour, outerPeriod.start).until(outerPeriod.end).toSet)
            } else {
              logger.trace(interval.resultGranularity.name + " series out of range")
              (true,Set(outerPeriod))
            }

            val (foundStats, foundPeriods) = found.foldLeft((List[(Option[String],ValueStats)](), Set[Period]())) {
              case ((stats,periods), cacheEntry) => {
                val entryStats = 
                  ValueStats((cacheEntry \ "count").deserialize[Long],
                             (cacheEntry \? "sum").map(_.deserialize[Double]),
                             (cacheEntry \? "sumsq").map(_.deserialize[Double]))

                (((cacheEntry \? "location").map(_.deserialize[String]), entryStats) :: stats, periods + Period(Periodicity.Hour, (cacheEntry \ "timestamp").deserialize[Instant]))
              }
            }

            val (disableCaching, periodsToQuery) = {
              val uncached = neededPeriods -- foundPeriods
              if (uncached.size > 1000 || outOfRange) {
                logger.info("Disabling caching for series: neede %d queries over \"%s\" periodicity".format(uncached.size, interval.resultGranularity.name))
                (true, Set(outerPeriod)) 
              } else (false, uncached)
            }
            
            logger.trace("Found %d periods, need to query %d: %s".format(foundPeriods.size, periodsToQuery.size, periodsToQuery))

            // A list of futures of lists of location/stats pairs...phew
            val queried : List[Future[List[(Option[String],ValueStats)]]] = periodsToQuery.toList.map {
              period => {
                def saveResult(vs: ValueStats, location: Option[String]) {
                  if (disableCaching || (outerPeriod.start isAfter now)) {
                    return
                  }

                  val toSave = JObject(List(Some(JField("accountTokenId", token.accountTokenId.serialize)),
                                            Some(JField("path", path.serialize)),
                                            Some(JField("timestamp", period.start.serialize)),
                                            Some(JField("where", whereClause.serialize)),
                                            Some(JField("count", vs.count.serialize)),
                                            location.map { l => JField("location", l.serialize) },
                                            vs.sum.map { s => JField("sum", s.serialize) },
                                            vs.sumsq.map { ss => JField("sumsq", ss.serialize) }).flatten)

                  logger.trace("Caching: " + toSave)

                  queryIndexdb(upsert(stats_cache_collection).set(toSave).where(JPath(".accountTokenId") === token.accountTokenId.serialize & JPath(".path") === path.path & (timestampField === period.start.getMillis) & JPath(".where") === whereClause & locationClause))
                }
                
                // If we have locations or prefixes, do location-based queries
                if (! (locations.isEmpty && anonPrefixes.isEmpty)) {
                  // Named locations can be queried directly
                  val namedLocResults: List[Future[List[(Option[String],ValueStats)]]] = locations.map {
                    case loc => {
                      logger.trace("varseries with named location")
                      val start = System.currentTimeMillis
                      aggregateVariableSeries(token, path, variable, interval.encoding, period, Seq(loc), additionalConstraints).map {
                        vs => {
                          logger.trace("aggr varseries for %s in %d ms".format(period, System.currentTimeMillis - start))
                          saveResult(vs, Some(loc.location.path.toString))
                          val result: List[(Option[String],ValueStats)] = List((Some(loc.location.path.toString),vs))
                          result
                        }
                      }
                    }
                  }

                  val anonLocResults: Future[List[(Option[String],ValueStats)]] = if (!anonPrefixes.isEmpty) {
                    logger.trace("varseries with anon locations")
                    mapReduceVariableSeries(token, path, variable, interval.encoding, period, additionalConstraints, anonPrefixes).map {
                      _.toList.map {
                        case (label, stats) => {
                          saveResult(stats, (label \? "location").map(_.deserialize[String]))
                          ((label \? "location").map(_.deserialize[String]),stats)
                        }
                      }
                    }
                  } else Future.sync(List[(Option[String],ValueStats)]()) 

                  Future((anonLocResults :: namedLocResults): _*).map(_.flatten)
                } else {
                  // No location tags, so we'll just aggregate all events for this period
                  logger.trace("varseries has no location, just timespan")
                  aggregateVariableSeries(token, path, variable, interval.encoding, period, Seq(), additionalConstraints).map {
                    vs => saveResult(vs, None); List((Option.empty[String],vs))
                  }
                }
              }
            }

            Future(queried: _*).map {
              queriedCounts => {
                val allCounts = if (disableCaching) queriedCounts else foundStats :: queriedCounts
                allCounts.flatten.groupBy { _._1 }.map { case (loc, stats) => {
                  (JObject(List(Some(JField("timestamp", outerPeriod.start.serialize)), loc.map(JField("location", _))).flatten),
                   stats.map{_._2}.reduceLeft(ValueStats.Ops.append(_,_)))
                }}.toList
              }
            }
          }
        }
      }
      // Bogus entry is added since time zone shifting will remove the first element. See AnalyticsService.shiftTimeSeries for details
    }: _*).map{ r => (List((JObject(JField("bogus", JBool(true)) :: Nil),ValueStats(0, None, None))) :: r).flatten.sortBy(_._1)(JObjectOrdering) }}

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

  // Given a list of { "_id" : { ... }, "value" : NumberLong(...) } values, compute the counts
  // "_id" object may contain more values than properties, in which case extra values are not considered and simply passed through
  private def computeIntersectionCounts(values: List[JObject], properties: List[VariableDescriptor]): ResultSet[JObject, CountType] = {
    if (values.isEmpty) {
      return List[(JObject,CountType)]()
    }

    // In order to handle sorting/top n, we need to know what our values actually are and how many times they appear
    var varValueBuckets = Map[String, Map[JValue, Long]]()
    
    values.foreach {
      case JObject(List(JField("_id", JObject(props)), JField("value", count))) => props.take(properties.size).foreach {
        case JField(fieldName, fieldValue) => {
          val valMap = varValueBuckets.get(fieldName).getOrElse(Map())
          varValueBuckets += (fieldName -> (valMap + (fieldValue -> (valMap.get(fieldValue).getOrElse(0l) + count.deserialize[Long]))))
        }
      }
      case _ => // Nothing to do for this case
    }

    // Use the buckets to compute the values we actually want in events
    val toKeep: Map[String, Set[JValue]] = properties.map {
      case v => v.sortOrder match {
        case SortOrder.Ascending  => (varSimpleName(v) -> varValueBuckets(varSimpleName(v)).toList.sortBy(_._2).take(v.maxResults).toMap.keySet)
        case SortOrder.Descending => (varSimpleName(v) -> varValueBuckets(varSimpleName(v)).toList.sortBy(-_._2).take(v.maxResults).toMap.keySet)
      }
    }.toMap

    logger.trace("aggregate-based keepers = " + toKeep)

    // Filter the results to ensure that only results with all properties in the keep set are used
    values.flatMap {
      case JObject(List(JField("_id", keys @ JObject(props)), JField("value", count))) => {
        // Only test the first N ID properties, where N is the number of request properties, but then pass the full array of ID field values
        if (props.take(properties.size).forall { case JField(name, value) => toKeep(name)(value) }) {
          Some((keys, count.deserialize[Long]))
        } else {
          None
        }
      }
      case invalid => logger.error("Invalid intersection count value = " + invalid); None
    }
  }

  private def varSimpleName(v : VariableDescriptor) = v.variable.name.nodes.last.toString.drop(1)

  private def varFullName(v: VariableDescriptor) = (JPath(".event.data") \ v.variable.name.tail).toString.drop(1) // Drop leading dot

  // This method is only used for queries with anon locations
  def getMRIntersectionCount(token: Token, path: Path, properties: List[VariableDescriptor], tagTerms: Seq[TagTerm], constraints: Set[HasValue]): Future[ResultSet[JArray, CountType]] = {
    val anonPrefixes = tagTerms.collect { case loc @ HierarchyLocationTerm(name, Hierarchy.AnonLocation(path)) => path.path }

    val emitter = anonLocMatchFunc + """
  var matchedPrefix = anonLocMatch(this['tags']['#location'], %1$s);

  if (matchedPrefix != null) {
    emit({ %2$s, "location" : matchedPrefix }, 1);
  }
""".format(anonPrefixes.mkString("[\"", "\",\"", "\"]"), properties.map { p => "\"%s\" : this.%s".format(varSimpleName(p), varFullName(p)) }.mkString(", "))

    val reducer = """
function (key, vals) {
  var count = 0;
  for (var i = 0; i < vals.length; i++) {
    count += vals[i].count;
  }
  return count;
}
"""

    // We drop the first variable since we'll be using it as the primary var for the queryAggregation call
    val fullConstraints : Set[HasValue] = (properties.drop(1).map { v => HasValue(v.variable, JNothing) } ++ constraints).toSet

    queryMapReduce(token, path, properties.head.variable, tagTerms, fullConstraints, emitter, reducer) {
      output => {
        queryEventsdb(selectAll.from(output.outputCollection)).map { _.toList }.map {
          values => {
            logger.trace("compiling MR-based count results")

            val finalResults = computeIntersectionCounts(values.map(_.asInstanceOf[JObject]), properties)

            logger.trace("MR-based final count results = " + finalResults)
            
            finalResults.map {
              case (JObject(props), count) => (JArray(props.map{ case JField(_, value) => value }), count)
            }
          }
        }
      }
    }
  }

  def getAggrIntersectionCount[T](token: Token, path: Path, properties: List[VariableDescriptor], tagTerms: Seq[TagTerm], constraints: Set[HasValue]): Future[List[JObject]] = {
    logger.trace("Running aggregate-based intersection count on " + properties.map(varSimpleName).mkString(", "))

    val pipeline = List(JObject(List(JField("$project", JObject(properties.map { v => JField(varFullName(v), JInt(1)) })))),
                        JObject(List(JField("$group",   JObject(List(JField("_id", JObject(properties.map { v => JField(varSimpleName(v), "$" + varFullName(v)) })),
                                                                     JField("value", JObject(List(JField("$sum", JInt(1)))))  ))))))

    // We drop the first variable since we'll be using it as the primary var for the queryAggregation call
    val fullConstraints : Set[HasValue] = (properties.drop(1).map { v => HasValue(v.variable, JNothing) } ++ constraints).toSet

    queryAggregation(token, path, properties.head.variable, tagTerms, fullConstraints, pipeline) {
      case Some(result) => {
        result("result") match {
          case JArray(values) => logger.trace("Intersection count result = " + values); Future.sync(values.collect { case jo : JObject => jo })
          case invalid => logger.error("Unexpected intersection count result: " + result); sys.error("Invalid intersection count result")
        }
      }
      case _ => Future.sync(Nil)
    }
  }

  def getIntersectionCount(token: Token, path: Path, properties: List[VariableDescriptor], tagTerms: Seq[TagTerm], constraints: Set[HasValue]): Future[ResultSet[JArray, CountType]] = {
    logger.trace("getIntersectionCount")

    if (hasAnonLocs(tagTerms)) {
      getMRIntersectionCount(token, path, properties, tagTerms, constraints)
    } else {
      getAggrIntersectionCount(token, path, properties, tagTerms, constraints).map {
        values => {
          logger.trace("compiling aggregate-based results")
          
          val finalResults = computeIntersectionCounts(values, properties)

          logger.trace("aggregate-based final results = " + finalResults)
            
          finalResults.map {
            case (JObject(props), count) => (JArray(props.map{ case JField(_, value) => value }), count)
          }
        }
      }
    }
  }

  def getIntersectionSeries(token: Token, path: Path, properties: List[VariableDescriptor], tagTerms: Seq[TagTerm], constraints: Set[HasValue]): Future[ResultSet[JArray, ResultSet[JObject, CountType]]] = {
    if (hasAnonLocs(tagTerms)) {
      getRawIntersectionSeries(token, path, properties, tagTerms, constraints)
    } else {
      val emptyResult : ResultSet[JArray, ResultSet[JObject, CountType]] = List()

      tagTerms.collectFirst { case i : IntervalTerm => (i, i.periods.toList) }.map {
        case (interval,periods) => {
          // Run the query over the intervals given and combine all of the periods together. We don't do any top/bottom limits at this point
          val results: Future[List[JObject]] = Future(periods.map {
            period => {
              val fullTerms = tagTerms ++ Seq(SpanTerm(interval.encoding, period.timeSpan))
              
              getAggrIntersectionCount(token, path, properties, fullTerms, constraints).map { _.map {
                case JObject(List(JField("_id", JObject(fields)), JField("value", count))) => {
                  JObject(List(JField("_id", fields ::: List(JField("timestamp", JInt(period.start.getMillis)))), JField("value", count))) 
                }
                case invalid => logger.error("Invalid intersect series result: " + invalid); sys.error("Invalid intersect series result")
              }}
            }
          }: _*).map(_.flatten)

          val output: Future[ResultSet[JArray, ResultSet[JObject, CountType]]] = results.map {
            values => {
              logger.trace("Intersection series raw results = " + values)

              val keptValues: ResultSet[JObject,CountType] = computeIntersectionCounts(values, properties)

              logger.trace("Intersection series kept = " + keptValues)

              val locationKeys: List[JField] = 
                tagTerms.collect {
                  case HierarchyLocationTerm(tagName, Hierarchy.NamedLocation(name, path)) => JField(tagName, path.path)
                }.toList

              val groups: Map[JObject, Map[JObject,CountType]] = keptValues.groupBy { case (JObject(ids), count) => JObject(ids.take(properties.length)) }.map {
                case (key, series) => (key, series.map { case (JObject(seriesKeys), count) => (JObject(seriesKeys.drop(properties.length) ::: locationKeys), count) }.toMap) // The series keys shouldn't contain the intersection keys and vice-versa
              }

              // Compute the full set of series keys based on periods and possibly locations
              val allSeriesKeys: List[JObject] = periods.map { p => JObject(JField("timestamp", JInt(p.start.getMillis)) :: locationKeys) }

              logger.trace("Intersection series processing " + groups + " over " + allSeriesKeys)

              groups.toList.map {
                case (keys, series) => {
                  val keyArray = JArray(keys.fields.map { case JField(_, value) => value })
                  
                  val fullSeries: ResultSet[JObject, CountType] = allSeriesKeys.map {
                    k => (k, series.getOrElse(k, 0l))
                  }
                  
                  (keyArray, fullSeries)
                }
              }
            }
          }
          output
        }
      }.getOrElse(Future.sync(emptyResult))
    }
  }

  private def getRawIntersectionSeries(token: Token, path: Path, variableDescriptors: List[VariableDescriptor], tagTerms: Seq[TagTerm], constraints: Set[HasValue]): Future[ResultSet[JArray, ResultSet[JObject, CountType]]] = {
    if (variableDescriptors.isEmpty) Future.async(Nil) else {
      val eventVariables: Map[String, List[VariableDescriptor]] = variableDescriptors.groupBy(_.variable.name.head.collect{ case JPathField(name) => name }.get)

      // We'll limit our query to just the fields we need to obtain the intersection, plus the event name
      val observations = JointObservation(constraints.toSeq ++ variableDescriptors.map(vd => HasValue(vd.variable, JNothing)): _*)

      val qid = java.util.UUID.randomUUID().toString

      logger.trace(qid + ": Querying mongo for intersection events with obs = " + observations)
      val queryStart = System.currentTimeMillis()

      getRawEvents(token, path, observations, tagTerms, Right(List(JPath(".event.name")))) map { events =>
          val evs = events.toList
          logger.trace("Total events = " + evs.size)

          val eventsByName = evs.groupBy(jv => (jv \ "event" \ "name").deserialize[String])

          logger.trace(qid + ": Got %d results for intersection after %d ms".format(eventsByName.map(_._2.size).sum, System.currentTimeMillis() - queryStart))

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

  // For convenience we define this here for reuse
  val timestampField = MongoFilterBuilder(JPath(".timestamp"))

  def rangeFor(tagTerms: Seq[TagTerm], filter: MongoFilter): Future[Option[(Instant,Instant)]] = {
    // Determine the time range in question so that we can check the cache for pre-computed values
    // We either have been given a time span or we need to query to determine the min/max for the token/path
    tagTerms.collectFirst[Future[Option[(Instant,Instant)]]] {
      case SpanTerm(encoding, TimeSpan(start, end)) => Future.sync(Some((start,end)))
    }.getOrElse {
      logger.warn("Resorting to full DB query to determine min/max timestamps")
      queryEventsdb(selectOne(".timestamp").from(events_collection).where(filter).sortBy(".timestamp" >>)).flatMap {
        case Some(start) => {
          queryEventsdb(selectOne(".timestamp").from(events_collection).where(filter).sortBy(".timestamp" <<)).flatMap {
            case Some(end) => Future.sync(Some((start("timestamp").deserialize[Instant]), new Instant(end("timestamp").deserialize[Long] + 1))) // make our end inclusive
            case None      => logger.warn("Found start timestamp without end during range query"); Future.sync(None) // Technically this shouldn't be possible if we found a start event
          }
        }
        case None => Future.sync(None) // Easy, there aren't any 
      }
    }
  }

  def cacheWhereClauseFor(tagTerms: Seq[TagTerm], variable: Variable, constraints: Set[HasValue]) = {
    val filters = List(tagTerms.collectFirst {
           case HierarchyLocationTerm(tagName, Hierarchy.NamedLocation(name, path)) => MongoFilterBuilder(JPath(".tags") \ ("#" + tagName) \ name) === path.path
           case HierarchyLocationTerm(tagName, Hierarchy.AnonLocation(path)) => (JPath(".tags") \ ("#" + tagName) === path.path)
         }, 
         eventNameFilter(variable),
         variableExistsFilter(variable),
         constraintFilter(constraints)).flatten

    if (filters.isEmpty) {
      JObject(Nil)
    } else {
      filters.reduceLeft(_ & _).filter.mapUp {
        case JField(name, value) => JField(name.replace(".", "_").replace("$",""), value)
        case other => other
      }
    }
  }

  def cacheFilterFor(token: Token, path: Path, whereClause: JValue, tagTerms: Seq[TagTerm], start: Long, end: Long, useLocation: Boolean) = {
    val baseCacheFilter : MongoFilter = 
      JPath(".accountTokenId") === token.accountTokenId.serialize & 
      JPath(".path") === path.path & 
      (timestampField >= start) & 
      (timestampField < end) & 
      JPath(".where") === whereClause

    if (useLocation) {
      locationClauseFor(tagTerms).map(baseCacheFilter & _).getOrElse(baseCacheFilter)
    } else {
      baseCacheFilter
    }
  }

  // Used to create the location clause for cache filtering based on provided tag terms
  def locationClauseFor(tagTerms: Seq[TagTerm]): Option[MongoFilter] = {
    val locations    = tagTerms.collect { case loc @ HierarchyLocationTerm(name, Hierarchy.NamedLocation(_,_)) => loc }.toList
    val anonPrefixes = tagTerms.collect { case loc @ HierarchyLocationTerm(name, Hierarchy.AnonLocation(path)) => path.path }

    /* This is where things get really ugly. While not technically limited, it makes no sense to have more than one location term,
     * so we'll just mash everything together and assume the end user isn't an idiot.*/
    if (! locations.isEmpty) {
      Some(locations.map { case HierarchyLocationTerm(name, loc) => JPath(".location") === loc.path.path }.reduceLeft[MongoFilter](_ & _))
    } else if (! anonPrefixes.isEmpty) {
      Some(anonPrefixes.map { path => JPath(".location").regex("^" + path) }.reduceLeft[MongoFilter](_ | _))
    } else {
      None
    }
  }

  def fullFilter(token: Token, path: Path, variable: Variable, tagTerms : Seq[TagTerm], where: Set[HasValue]) = {
    val fullFilterParts = List(Some(baseFilter(token, path)), 
                               tagsFilter(tagTerms), 
                               eventNameFilter(variable),
                               variableExistsFilter(variable),
                               constraintFilter(where))
    
    logger.trace("Full filter components = " + fullFilterParts.flatten)

    fullFilterParts.flatten.reduceLeft(_ & _)
  }

  // the base filter is using a prefix-based match strategy to catch the path. This has the implication that paths are rolled up 
  // by default, which may not be exactly what we want. Hence, we later have to filter the returned results for only those where 
  // the rollup depth is greater than or equal to the difference in path length.
  private def baseFilter(token: Token, path: Path): MongoFilter = {
    JPath(".accountTokenId") === token.accountTokenId.serialize & JPath(".path").regex("^" + path.path) & MongoFilterBuilder(JPath(".min_ru")) <= path.length
  }

  private def tagsFilter(tagTerms: Seq[TagTerm]): Option[MongoFilter] = {
    val tagsFilters: List[MongoFilter]  = tagTerms.collect {
      case IntervalTerm(_, _, span) => 
        (MongoFilterBuilder(JPath(".timestamp")) >= span.start.getMillis) & 
        (MongoFilterBuilder(JPath(".timestamp")) <  span.end.getMillis)

      case SpanTerm(_, span) =>
        (MongoFilterBuilder(JPath(".timestamp")) >= span.start.getMillis) & 
        (MongoFilterBuilder(JPath(".timestamp")) <  span.end.getMillis)

      case HierarchyLocationTerm(tagName, Hierarchy.NamedLocation(name, path)) =>
        MongoFilterBuilder(JPath(".tags") \ ("#" + tagName) \ name) === path.path
    }.toList

    (anonLocationFilter(tagTerms).map(_ :: tagsFilters).getOrElse(tagsFilters)).reduceLeftOption(_ & _)
  }

  def anonLocationFilter(tagTerms: Seq[TagTerm]): Option[MongoFilter] = {
    val prefixes : List[String] = tagTerms.collect {
      case HierarchyLocationTerm(tagName, Hierarchy.AnonLocation(path)) => path.toString
    }.toList

    if (prefixes.isEmpty) {
      None
    } else {
      val rawJS = """var loc = this['tags']['#location']; for (var prop in loc) { var locv = loc[prop]; if (%s.some(function(p){ return p == locv; })) return true; }; return false;""".format(prefixes.mkString("[\"", "\",\"", "\"]"))
      Some(evaluation(rawJS) & JPath(".tags.#location").isDefined)
    }
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
    val filters : Set[MongoFilter] = constraints.flatMap {
      case HasValue(variable, JNothing)    => variableExistsFilter(variable)
      case HasValue(Variable(name), value) => Some((JPath(".event.data") \ name.tail) === value)
    }

    filters.reduceLeftOption(_ & _)
  }

  private def hasAnonLocs(tagTerms: Seq[TagTerm]): Boolean = 
    tagTerms.collectFirst { case loc @ HierarchyLocationTerm(_, Hierarchy.AnonLocation(_)) => true }.isDefined

  private def hasNamedLocs(tagTerms: Seq[TagTerm]): Boolean = 
    tagTerms.collectFirst { case loc @ HierarchyLocationTerm(_, Hierarchy.NamedLocation(_,_)) => true }.isDefined

  private def rawEventsVariableFilter(token: Token, path: Path, variable: Variable, tagTerms: Seq[TagTerm]): MongoFilter = {
    List(Some(baseFilter(token, path)),
         tagsFilter(tagTerms),
         eventNameFilter(variable),
         variableExistsFilter(variable)).flatten.reduceLeft(_ & _)
  }

  def getRawEvents(token: Token, path: Path, observation: JointObservation[HasValue], tagTerms: Seq[TagTerm], extraFields : Either[Boolean,List[JPath]] = Left(false)) : Future[MongoSelectQuery#QueryResult] = {
    // the base filter is using a prefix-based match strategy to catch the path. This has the implication that paths are rolled up 
    // by default, which may not be exactly what we want. Hence, we later have to filter the returned results for only those where 
    // the rollup depth is greater than or equal to the difference in path length.
    val rawBaseFilter = baseFilter(token, path)

    // Some tags can be filtered. We can no longer pre-filter for location, since location is free-form in raw events
    val rawTagsFilter = tagsFilter(tagTerms).map(rawBaseFilter & _).getOrElse(rawBaseFilter)

    // We always need path and rollup and possibly need timestamp and/or location if we have proper tags
    val baseFieldsToRetrieve : List[JPath] = tagTerms.flatMap {
      case i : IntervalTerm          => Some(JPath(".timestamp"))
      case s : SpanTerm              => Some(JPath(".timestamp"))
      case l : HierarchyLocationTerm => Some(JPath(".tags.#location"))
      case _                         => None
    }.distinct.toList

    val (neededFields,filter) = observation.obs.foldLeft((baseFieldsToRetrieve, rawTagsFilter)) {
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

    retrieveEventsFor(filter, fieldsToRetrieve)
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
        case (docKey, dataKeys) => queryIndexdb(selectOne(dataPath).from(collection).where(filter(docKey))).map {
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
    queryIndexdb {
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
    queryIndexdb {
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
    ),
    "stats_cache" -> Map(
      "stats_cache_query" -> (List("accountTokenId", "path", "where"), false)
    ),
    "histo_cache" -> Map(
      "histo_cache_query" -> (List("accountTokenId", "path", "where"), false)
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

  def apply(config: ConfigMap, logger: Logger, insertEventsdb: Database, queryEventsdb: Database, queryIndexdb: Database, healthMonitor: HealthMonitor)(implicit hashFunction: HashFunction = Sha1HashFunction): Future[AggregationEngine] = {
    for {
      _ <- createIndices(queryEventsdb, EventsdbIndices)
      _ <- createIndices(queryIndexdb,  IndexdbIndices)
    } yield {
      new AggregationEngine(config, logger, insertEventsdb, queryEventsdb, queryIndexdb, ClockSystem.realtimeClock, healthMonitor)
    }
  }

  def forConsole(config: ConfigMap, logger: Logger, insertEventsdb: Database, queryEventsdb: Database, queryIndexdb: Database, healthMonitor: HealthMonitor)(implicit hashFunction: HashFunction = Sha1HashFunction) = {
    new AggregationEngine(config, logger, insertEventsdb, queryEventsdb, queryIndexdb, ClockSystem.realtimeClock, healthMonitor)
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
