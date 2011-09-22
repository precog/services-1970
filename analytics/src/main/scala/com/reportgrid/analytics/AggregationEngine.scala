package com.reportgrid.analytics

import blueeyes._
import blueeyes.concurrent._
import blueeyes.persistence.mongo._
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
import net.lag.logging.Logger

import org.joda.time.Instant

import scala.collection.SortedMap
import scalaz.Scalaz._
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

class AggregationEngine private (config: ConfigMap, logger: Logger, database: Database, clock: Clock)(implicit hashFunction: HashFunction = Sha1HashFunction) {
  import AggregationEngine._

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

  private val events_collection: MongoCollection = config.getString("events.collection", "events")

  def store(token: Token, path: Path, eventName: String, eventBody: JObject, count: Int) = {
    val async = Future.async {
      val tagsFuture: Future[List[Tag]] = Tag.extractTimestampTag(timeSeriesEncoding, "timestamp", eventBody \ Tag.TimestampProperty) match {
        case Tag.Tags(tags) => tags.map(_.toList)
        case _ =>              Future.sync(Tag("timestamp", TimeReference(timeSeriesEncoding, clock.instant)) :: Nil)
      } 
      
      tagsFuture.flatMap[Unit] { 
        case Tag(_, TimeReference(_, instant)) :: Nil => 
          val record = JObject(
            JField("token",     token.tokenId.serialize) ::
            JField("path",      path.serialize) ::
            JField("event",     JObject(JField("name", eventName) :: JField("data", eventBody) :: Nil)) :: 
            JField("count",     count.serialize) ::
            JField("timestamp", instant.serialize) :: Nil
          )
        
          database(MongoInsertQuery(events_collection, List(record))) 
          
        case _ => 
          Future.dead(new IllegalStateException("Unexpected system state; please report error RG-AE0"))
      }
    } 

    async.flatten
  }

  /** 
   * Add the event data for the specified event to the database
   */
  def aggregate(token: Token, path: Path, eventName: String, allTags: Seq[Tag], eventBody: JObject, count: Int) = Future.async {
    import token.limits.{order, depth, limit}

    val tags = allTags.take(token.limits.tags)
    val event = JObject(JField(eventName, eventBody) :: Nil)

    // Keep track of parent/child relationships:
    path_children putAll addPathChildrenOfPath(token, path).patches
    path_children put addChildOfPath(forTokenAndPath(token, path), "." + eventName)

    val (finiteObs, infiniteObs) = JointObservations.ofValues(event, order, depth, limit)
    val finiteOrder1 = finiteObs.filter(_.order == 1)

    variable_values putAll variableValuesPatches(token, path, finiteOrder1.flatMap(_.obs), count).patches

    val (vvSeriesPatches, vvInfinitePatches) = variableValueSeriesPatches(token, path, Report(tags, finiteObs), infiniteObs, count)
    variable_value_series    putAll vvSeriesPatches.patches
    variable_values_infinite putAll vvInfinitePatches.patches 

    val childObservations = JointObservations.ofChildren(event, 1)
    variable_children putAll variableChildrenPatches(token, path, childObservations.flatMap(_.obs), count).patches

    val childSeriesReport = Report(tags, (JointObservations.ofInnerNodes(event, 1) ++ finiteOrder1 ++ infiniteObs.map(JointObservation(_))).map(_.of[Observation]))
    variable_series putAll variableSeriesPatches(token, path, childSeriesReport, count).patches
  }

  /** Retrieves children of the specified path &amp; variable.  */
  def getVariableChildren(token: Token, path: Path, variable: Variable): Future[List[HasChild]] = {
    extractValues(forTokenAndPath(token, path) & forVariable(variable), variable_children.collection) { (jvalue, _) =>
      HasChild(variable, jvalue.deserialize[JPathNode])
    }
  }

  /** Retrieves children of the specified path.  */
  def getPathChildren(token: Token, path: Path): Future[List[String]] = {
    extractChildren(forTokenAndPath(token, path), path_children.collection) { (jvalue, _) =>
      jvalue.deserialize[String]
    }
  }
  
  def getHistogram(token: Token, path: Path, variable: Variable): Future[Map[JValue, CountType]] = 
    getHistogramInternal(token, path, variable)

  def getHistogramTop(token: Token, path: Path, variable: Variable, n: Int): Future[ResultSet[JValue, CountType]] = 
    getHistogramInternal(token, path, variable).map(v => v.toList.sortBy(- _._2).take(n))

  def getHistogramBottom(token: Token, path: Path, variable: Variable, n: Int): Future[ResultSet[JValue, CountType]] = 
    getHistogramInternal(token, path, variable).map(v => v.toList.sortBy(_._2).take(n))

  /** Retrieves values of the specified variable.
   */
  def getValues(token: Token, path: Path, variable: Variable): Future[Iterable[JValue]] = 
    getHistogramInternal(token, path, variable).map(_.map(_._1))

  def getValuesTop(token: Token, path: Path, variable: Variable, n: Int): Future[Seq[JValue]] = 
    getHistogramTop(token, path, variable, n).map(_.map(_._1))

  def getValuesBottom(token: Token, path: Path, variable: Variable, n: Int): Future[Seq[JValue]] = 
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
  def getVariableCount(token: Token, path: Path, variable: Variable, tagTerms: Seq[TagTerm]): Future[CountType] = {
    getVariableSeries(token, path, variable, tagTerms).map {
      _.mapValues(_.count).total
    }
  }

  /** Retrieves a time series of statistics of occurrences of the specified variable in a path */
  def getVariableSeries(token: Token, path: Path, variable: Variable, tagTerms: Seq[TagTerm]): Future[ResultSet[JObject, ValueStats]] = {
    internalSearchSeries[ValueStats](
      tagTerms,
      variableSeriesKey(token, path, _, variable), 
      DataPath, variable_series.collection)
  }

  /** Retrieves a count of the specified observed state over the given time period */
  def getObservationCount(token: Token, path: Path, observation: JointObservation[HasValue], tagTerms: Seq[TagTerm]): Future[CountType] = {
    getObservationSeries(token, path, observation, tagTerms) map (_.total)
  }

  /** Retrieves a time series of counts of the specified observed state
   *  over the given time period.
   */
  def getObservationSeries(token: Token, path: Path, observation: JointObservation[HasValue], tagTerms: Seq[TagTerm]): Future[ResultSet[JObject, CountType]] = {
    internalSearchSeries[CountType](
      tagTerms,
      valueSeriesKey(token, path, _, observation), 
      CountsPath, variable_value_series.collection)
  }

  def getIntersectionCount(token: Token, path: Path, properties: List[VariableDescriptor], tagTerms: Seq[TagTerm]): Future[ResultSet[JArray, CountType]] = {
    getIntersectionSeries(token, path, properties, tagTerms) map {
      _.foldLeft(SortedMap.empty[JArray, CountType](JArrayOrdering)) {
        case (total, (key, timeSeries)) => 
          total + (key -> total.get(key).map(_ |+| timeSeries.total).getOrElse(timeSeries.total))
      }.toSeq
    } 
  }

  def getIntersectionSeries(token: Token, path: Path, variableDescriptors: List[VariableDescriptor], tagTerms: Seq[TagTerm]): Future[ResultSet[JArray, ResultSet[JObject, CountType]]] = {
    val variables = variableDescriptors.map(_.variable)

    val futureHistograms: Future[List[Map[JValue, CountType]]] = Future {
      variableDescriptors.map { 
        case VariableDescriptor(variable, maxResults, SortOrder.Ascending) =>
          getHistogramBottom(token, path, variable, maxResults).map(_.toMap)

        case VariableDescriptor(variable, maxResults, SortOrder.Descending) =>
          getHistogramTop(token, path, variable, maxResults).map(_.toMap)
      }: _*
    }

    futureHistograms.flatMap { histograms  => 
      implicit val resultOrder = intersectionOrder(variableDescriptors.map(_.sortOrder) zip histograms)

      def observations(vs: List[Variable]): Iterable[JointObservation[HasValue]] = {
        def obs(i: Int, variable: Variable, vs: List[Variable], o: Set[HasValue]): Iterable[Set[HasValue]] = {
          histograms(i).flatMap { 
            case (jvalue, _) => vs match {
              case Nil => List(o + HasValue(variable, jvalue))
              case vv :: vvs => obs(i + 1, vv, vvs, o + HasValue(variable, jvalue))
            }
          }
        }

        vs match {
          case Nil => Nil
          case v :: vs => obs(0, v, vs, Set.empty[HasValue]).map(JointObservation(_))
        }
      }

      Future {
        observations(variables).map { joint => 
          val obsMap: Map[Variable, JValue] = joint.obs.map(hv => hv.variable -> hv.value)(collection.breakOut)
          getObservationSeries(token, path, joint, tagTerms).map { result => 
            (JArray(variables.flatMap(obsMap.get).toList) -> result)
          }
        }.toSeq: _*
      } map {
        _.foldLeft(SortedMap.empty[JArray, ResultSet[JObject, CountType]]) {
          case (results, (k, v)) => 
            results + (k -> results.get(k).map(_ |+| v).getOrElse(v))
        }.toSeq
      }
    }
  }

  def findRelatedInfiniteValues(token: Token, path: Path, observation: JointObservation[HasValue], span: TimeSpan): Future[List[HasValue]] = {
    def find(intervalTerm: IntervalTerm) = Future {
      intervalTerm.infiniteValueKeys.map { timeKey =>
        val refKey = valueSeriesKey(token, path, Set(timeKey).sig, observation).rhs
        database(
          select(".variable", ".value")
          .from(variable_values_infinite.collection)
          .where(".ids" === refKey) //array comparison in Mongo uses equality for set inclusion. Go figure.
        ) 
      }: _*
    } 

    searchQueriableExpansion(span, find) map {
      _.flatten.flatMap { 
        _.map(jobj => HasValue((jobj \ "variable").deserialize[Variable], (jobj \ "value")))
      }
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
        case (docKey, dataKeys) => database(selectOne(dataPath).from(collection).where(filter(docKey))).map {
          results => dataKeys.flatMap {
            case (keySig, keyData) => results.map {
               // since the data keys are a stream of all possible keys in the document that data might
               // be found under, and since we want to return a zero-filled result set, we use the \? form of
               // the object find method and then use getOrElse returning the zero for T if no such key is found.
               jobj => keyData -> (jobj(dataPath) \? keySig.hashSignature).map(_.deserialize[T]).getOrElse(mzero[T])
            }
          }
        }
      }.toSeq: _*
    ) map {
      _.flatten
    }
  }

  private def valuesKeyFilter(token: Token, path: Path, variable: Variable) = {
    JPath("." + valuesId) === Sig(token.sig, path.sig, variable.sig).hashSignature
  }

  /** Retrieves a histogram of the values a variable acquires over its lifetime.
   */
  private def getHistogramInternal(token: Token, path: Path, variable: Variable): Future[Map[JValue, CountType]] = {
    if (variable.name.endsInInfiniteValueSpace) {    
      sys.error("Cannot obtain a histogram of a variable with a potentially infinite number of values.")
    }

    getVariableLength(token, path, variable).flatMap { 
      case 0 =>
        val extractor = extractValues[(JValue, CountType)](valuesKeyFilter(token, path, variable), variable_values.collection) _
        extractor((jvalue, count) => (jvalue, count)) map (_.toMap)

      case length =>
        val futures = for (index <- 0 until length) yield {
          getHistogramInternal(token, path, Variable(variable.name \ JPathIndex(index)))
        }

        for (results <- Future(futures: _*)) yield results.foldLeft(Map.empty[JValue, CountType])(_ |+| _)
    }    
  }

  private def infiniteSeriesKey(token: Token, path: Path, hasValue: HasValue) = {
    JPath("." + seriesId) === Sig(token.sig, path.sig, hasValue.sig).hashSignature
  }

  private def valueSeriesKey(token: Token, path: Path, tagsSig: Sig, observation: JointObservation[HasValue]) = {
    JPath("." + seriesId) === Sig(token.sig, path.sig, tagsSig, observation.sig).hashSignature
  }

  private def variableSeriesKey(token: Token, path: Path, tagsSig: Sig, variable: Variable) = {
    JPath("." + seriesId) === Sig(token.sig, path.sig, tagsSig, variable.sig).hashSignature
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
  private def variableValuesPatches(token: Token, path: Path, observations: Set[HasValue], count: CountType): MongoPatches = {
    observations.foldLeft(MongoPatches.empty) { 
      case (patches, HasValue(variable, value)) =>
        val predicateField = MongoEscaper.encode(renderNormalized(value.serialize))
        val valuesUpdate = (JPath(".values") \ JPathField(predicateField)) inc count

        patches + (valuesKeyFilter(token, path, variable) -> valuesUpdate)
    }
  }

  /** Creates patches to record variable observations.
   */
  private def variableChildrenPatches(token: Token, path: Path, observations: Set[HasChild], count: CountType): MongoPatches = {
    observations.foldLeft(MongoPatches.empty) { 
      case (patches, HasChild(variable, child)) =>

        val filterVariable = forTokenAndPath(token, path) & forVariable(variable)
        val predicateField = MongoEscaper.encode(renderNormalized(child.serialize))

        val valuesUpdate = (JPath(".values") \ JPathField(predicateField)) inc count

        patches + (filterVariable -> valuesUpdate)
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

  private def variableValueSeriesPatches(token: Token, path: Path, finiteReport: Report[HasValue], infiniteObs: Set[HasValue], count: CountType): (MongoPatches, MongoPatches) = {
    val (finitePatches, refKeys) = finiteReport.storageKeysets.foldLeft((MongoPatches.empty, List.empty[MongoPrimitive])) {
      case ((finitePatches, refKeys), (storageKeys, finiteObservation)) => 

        // When we query for associated infinite values, we will use the queriable expansion (heap shaped) 
        // of the specified time series so here we need to construct a value series key that records where 
        // the observation was made(as described by the data keys of the storage keys), not where it is 
        // being stored. We will then construct an identical key when we query, and the fact that we have 
        // built aggregated documents will mean that we query for the minimal set of infinite value documents.
        val (dataKeySig, refSig) = dataKeySigs(storageKeys)
        val referenceKey = valueSeriesKey(token, path, refSig, finiteObservation).rhs
        val docKey = valueSeriesKey(token, path, docKeySig(storageKeys), finiteObservation)

        Tuple2(
          finitePatches + (docKey -> ((CountsPath \ dataKeySig.hashSignature) inc count)),
          referenceKey :: refKeys
        )
    } 

    //build infinite patches by adding the finite key id to the list of locations that the infinite value
    //was observed in conjunction with.
    val infinitePatches = infiniteObs.foldLeft(MongoPatches.empty) {
      case (patches, hasValue) => patches + (
        infiniteSeriesKey(token, path, hasValue) -> {
          (MongoUpdateBuilder(".ids")      pushAll(refKeys: _*)) |+| 
          (MongoUpdateBuilder(".variable") set  hasValue.variable.serialize) |+| 
          (MongoUpdateBuilder(".value")    set  hasValue.value.serialize) |+| 
          (MongoUpdateBuilder(".count")    inc  count)
        }
      )
    }

    (finitePatches, infinitePatches)
  }

  private def variableSeriesPatches(token: Token, path: Path, report: Report[Observation], count: CountType): MongoPatches = {
    def countUpdate(sig: Sig): MongoUpdate = (DataPath \ sig.hashSignature \ "count") inc count

    def statsUpdate(variable: Variable, jvalue: JValue, sig: Sig): MongoUpdate = {
      import scala.math.BigInt._

      // build a patch including the count, sum, and sum of the squares of integral values
      val dataPath = DataPath \ sig.hashSignature
      val updates = List(
        Some(countUpdate(sig)),
        jvalue option {
          case JInt(i)    => (dataPath \ "sum") inc (count * i)
          case JDouble(i) => (dataPath \ "sum") inc (count * i)
          case JBool(i)   => (dataPath \ "sum") inc (count * (if (i) 1 else 0))
        },
        jvalue option {
          case JInt(i)    => (dataPath \ "sumsq") inc (count * (i * i))
          case JDouble(i) => (dataPath \ "sumsq") inc (count * (i * i))
        }, 
        jvalue option {
          case JString(s) if !variable.name.endsInInfiniteValueSpace => 
            (dataPath \ "values" \ MongoEscaper.encode(s)) inc count
        }
      )
      
      updates.flatten.foldLeft(MongoUpdate.Empty)(_ |+| _)
    }

    report.storageKeysets.foldLeft(MongoPatches.empty) {
      case (patches, (storageKeys, joint)) => 
        val (dataKeySig, _) = dataKeySigs(storageKeys)
        
        joint.obs.foldLeft(patches) { 
          case (patches, HasChild(Variable(vpath), child)) => 
            val key = variableSeriesKey(token, path, docKeySig(storageKeys), Variable(vpath \ child))
            patches + (key -> countUpdate(dataKeySig))

          case (patches, HasValue(variable, jvalue)) =>             
            val key = variableSeriesKey(token, path, docKeySig(storageKeys), variable)
            patches + (key -> statsUpdate(variable, jvalue, dataKeySig))
        }
    }
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
  type CountType = Long
  type ResultSet[K <: JValue, V] = Seq[(K, V)]

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
      "var_val_inf_id" -> (List(valuesId, "value"), true) //,
    //  "var_val_inf_keyMatch" -> (List("ids"), false)
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
      database {
        if (unique) ensureUniqueIndex(indexName + "_index").on(fields.map(JPath(_)): _*).in(collection)
        else ensureIndex(indexName + "_index").on(fields.map(JPath(_)): _*).in(collection)
      }.toUnit
    }

    Future(futures.toSeq: _*)
  }

  def apply(config: ConfigMap, logger: Logger, database: Database)(implicit hashFunction: HashFunction = Sha1HashFunction): Future[AggregationEngine] = {
    createIndices(database).map(_ => new AggregationEngine(config, logger, database, ClockSystem.realtimeClock))
  }

  def forConsole(config: ConfigMap, logger: Logger, database: Database)(implicit hashFunction: HashFunction = Sha1HashFunction) = {
    new AggregationEngine(config, logger, database, ClockSystem.realtimeClock)
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
}
