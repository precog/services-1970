package com.reportgrid.analytics

import blueeyes._
import blueeyes.core.http.{HttpStatus, HttpResponse, MimeTypes}
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.concurrent.test.FutureMatchers
import blueeyes.health.HealthMonitor
import blueeyes.util._
import blueeyes.util.metrics.Duration
import blueeyes.util.metrics.Duration.toDuration
import MimeTypes._

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JPathImplicits._
import blueeyes.json.xschema.JodaSerializationImplicits._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.persistence.mongo._
import blueeyes.concurrent.Future

import net.lag.configgy.{Configgy, Config, ConfigMap}
import net.lag.logging.Logger

import org.joda.time.Instant
import org.specs2.mutable.Specification
import org.specs2.specification.{Outside, Scope}
import org.specs2.matcher.MatchResult
import org.specs2.execute.Result
import org.scalacheck._
import scala.math.Ordered._
import Gen._
import scalaz._
import Scalaz._
import Periodicity._

trait LocalMongo {
  val eventsName = "testev" + scala.util.Random.nextInt(10000)
  val indexName =  "testix" + scala.util.Random.nextInt(10000)

  def mongoConfigFileData = """
    eventsdb {
      database = "%s"
      servers  = ["localhost:27017"]
    }

    indexdb {
      database = "%s"
      servers  = ["localhost:27017"]
    }

    tokens {
      collection = "tokens"
    }

    variable_series {
      collection = "variable_series"
      time_to_idle_millis = 100
      time_to_live_millis = 100

      initial_capacity = 100
      maximum_capacity = 100
    }

    variable_value_series {
      collection = "variable_value_series"

      time_to_idle_millis = 100
      time_to_live_millis = 100

      initial_capacity = 100
      maximum_capacity = 100
    }

    variable_values {
      collection = "variable_values"

      time_to_idle_millis = 100
      time_to_live_millis = 100

      initial_capacity = 100
      maximum_capacity = 100
    }

    variable_children {
      collection = "variable_children"

      time_to_idle_millis = 100
      time_to_live_millis = 100

      initial_capacity = 100
      maximum_capacity = 100
    }

    path_children {
      collection = "path_children"

      time_to_idle_millis = 100
      time_to_live_millis = 100

      initial_capacity = 100
      maximum_capacity = 100
    }

    log {
      level   = "warning"
      console = true
    }
  """.format(eventsName, indexName)
}

object FutureUtils {
  def get[A](f: Future[A]): A = {
    val latch = new java.util.concurrent.CountDownLatch(1)

    f.deliverTo(_ => latch.countDown())

    latch.await()

    f.value.get
  }
}

trait AggregationEngineTests extends Specification with FutureMatchers with ArbitraryEvent {
  val TestToken = Token(
    tokenId        = "C7A18C95-3619-415B-A89B-4CE47693E4CC",
    parentTokenId  = Some(Token.Root.tokenId),
    accountTokenId = "C7A18C95-3619-415B-A89B-4CE47693E4CC",
    path           = "unittest",
    permissions    = Permissions(true, true, true, true),
    expires        = Token.Never,
    limits         = Limits(order = 1, depth = 5, limit = 20, tags = 2, rollup = 2)
  )

  def countStoredEvents(sampleEvents: List[Event], engine: AggregationEngine) = {
    //skip("disabled")
    def countEvents(eventName: String) = sampleEvents.count {
      case Event(name, _, _) => name == eventName
    }

    val eventCounts = EventTypes.map(eventName => (eventName, countEvents(eventName))).toMap

    val queryTerms = List[TagTerm](
      HierarchyLocationTerm("location", Hierarchy.AnonLocation(com.reportgrid.analytics.Path("usa")))
    )

    forall(eventCounts) {
      case (name, count) => engine.getVariableCount(TestToken, "/test", Variable("." + name), queryTerms) must whenDelivered(be_==(count))
    }
  }
}

class AggregationEngineSpec extends AggregationEngineTests with LocalMongo {
  import AggregationEngine._
  import FutureUtils._

  val genTimeClock = Clock.System

  val config = (new Config()) ->- (_.load(mongoConfigFileData))

  val eventsConfig = config.configMap("eventsdb")
  val eventsMongo = new RealMongo(eventsConfig) 
  val eventsdb = eventsMongo.database(eventsConfig("database"))
  
  val indexConfig = config.configMap("indexdb")
  val indexMongo = new RealMongo(indexConfig)
  val indexdb = indexMongo.database(indexConfig("database"))

  val engine = get(AggregationEngine(config, Logger.get, eventsdb, indexdb, HealthMonitor.Noop))

  override implicit val defaultFutureTimeouts = FutureTimeouts(15, toDuration(1000).milliseconds)

  object sampleData extends Outside[List[Event]] with Scope {
    val outside = containerOfN[List, Event](30, fullEventGen).sample.get ->- {
      _.foreach { event => 
        engine.aggregate(TestToken, "/test", event.eventName, event.tags, event.data, 1)
        engine.store(TestToken, "/test", event.eventName, event.messageData, Tag.Tags(Future.sync(event.tags)), 1, 0, false)
      }
    }
  }

  "Aggregating full events" should {
    "retrieve path children" in sampleData { sampleEvents =>
      //skip("disabled")
      val children = sampleEvents.map { case Event(eventName, _, _) => "." + eventName }.toSet

      engine.getPathChildren(TestToken, "/test") must whenDelivered {
        haveTheSameElementsAs(children)
      }
    }
 
    "retrieve variable children" in sampleData { sampleEvents =>
      //skip("disabled")
      val expectedChildren = sampleEvents.foldLeft(Map.empty[String, Set[String]]) {
        case (m, Event(eventName, EventData(JObject(fields)), _)) => 
          val properties = fields.map("." + _.name)
          m + (eventName -> (m.getOrElse(eventName, Set.empty[String]) ++ properties))
      }

      expectedChildren forall { 
        case (eventName, children) => 
          engine.getVariableChildren(TestToken, "/test", Variable(JPath("." + eventName))).map(_.map(_.child.toString)) must whenDelivered {
            haveTheSameElementsAs(children)
          }
      }
    }

    "count events" in sampleData { sampleEvents =>
      def countEvents(eventName: String) = sampleEvents.count {
        case Event(name, _, _) => name == eventName
      }

      val queryTerms = List[TagTerm](
        HierarchyLocationTerm("location", Hierarchy.AnonLocation(com.reportgrid.analytics.Path("usa")))
      )

      forall(EventTypes.map(eventName => (eventName, countEvents(eventName)))) {
        case (name, count) => 
          engine.getVariableCount(TestToken, "/test", Variable("." + name), queryTerms) must whenDelivered {
            be_==(count)
          }
      }
    }
 
    "retrieve values" in sampleData { sampleEvents =>
      //skip("disabled")
      val values = sampleEvents.foldLeft(Map.empty[(String, JPath), Set[JValue]]) { 
        case (map, Event(eventName, obj, _)) =>
          obj.flattenWithPath.foldLeft(map) {
            case (map, (path, value)) if (!path.endsInInfiniteValueSpace) => 
              val key = (eventName, path)
              val oldValues = map.getOrElse(key, Set.empty)

              map + (key -> (oldValues + value))
            case (map, _) => map
          }

        case (map, _) => map
      }

      forall(values) {
        case ((eventName, path), values) => 
          engine.getValues(TestToken, "/test", Variable(JPath(eventName) \ path)) must whenDelivered {
            haveTheSameElementsAs(values)
          }
      }
    }

    "retrieve all values of arrays" in sampleData { sampleEvents =>
      //skip("disabled")
      val arrayValues = sampleEvents.foldLeft(Map.empty[(String, JPath), Set[JValue]]) { 
        case (map, Event(eventName, obj, _)) =>
          map <+> ((obj.children.collect { case JField(name, JArray(elements)) => ((eventName, JPath(name)), elements.toSet) }).toMap)

        case (map, _) => map
      }

      forall(arrayValues) {
        case ((eventName, path), values) =>
          engine.getValues(TestToken, "/test", Variable(JPath(eventName) \ path)) must whenDelivered {
            haveTheSameElementsAs(values)
          }
      }
    }

    "retrieve the top results of a histogram" in sampleData { sampleEvents => 
      //skip("disabled")
      val retweetCounts = sampleEvents.foldLeft(Map.empty[JValue, Int]) {
        case (map, Event("tweeted", data, _)) => 
          val key = data.value(".retweet")
          map + (key -> map.get(key).map(_ + 1).getOrElse(1))

        case (map, _) => map
      }

      engine.getHistogramTop(TestToken, "/test", Variable(".tweeted.retweet"), 10) must whenDelivered {
        haveTheSameElementsAs(retweetCounts)
      }
    }

    "retrieve totals" in sampleData { sampleEvents =>
      //skip("disabled")
      val expectedTotals = valueCounts(sampleEvents).filterNot {
        case ((jpath, _), _) => jpath.endsInInfiniteValueSpace
      }

      val queryTerms = List[TagTerm](
        HierarchyLocationTerm("location", Hierarchy.AnonLocation(com.reportgrid.analytics.Path("usa")))
      )

      forall(expectedTotals) {
        case ((jpath, value), count) =>
          engine.getObservationCount(TestToken, "/test", JointObservation(HasValue(Variable(jpath), value)), queryTerms) must whenDelivered {
            be_==(count.toLong)
          }
      }
    }

    "retrieve a time series for occurrences of a variable" in sampleData { sampleEvents =>
      //skip("disabled")
      val granularity = Minute
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)

      val queryTerms = List[TagTerm](
        IntervalTerm(AggregationEngine.timeSeriesEncoding, granularity, TimeSpan(minDate, maxDate)),
        HierarchyLocationTerm("location", Hierarchy.AnonLocation(com.reportgrid.analytics.Path("usa")))
      )

      val expectedTotals = events.foldLeft(Map.empty[JPath, Int]) {
        case (map, Event(eventName, obj, _)) =>
          obj.flattenWithPath.foldLeft(map) {
            case (map, (path, _)) =>
              val key = JPath(eventName) \ path
              map + (key -> (map.getOrElse(key, 0) + 1))
          }
      }

      forall(expectedTotals) {
        case (jpath, count) =>
          engine.getVariableSeries(TestToken, "/test", Variable(jpath), queryTerms).map(_.total.count) must whenDelivered {
            be_==(count.toLong)
          }
      }
    }

    "retrieve a time series of means of values of a variable" in sampleData { sampleEvents =>
      //skip("disabled")
      val granularity = Minute
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)

      val queryTerms = List[TagTerm](
        IntervalTerm(AggregationEngine.timeSeriesEncoding, granularity, TimeSpan(minDate, maxDate)),
        HierarchyLocationTerm("location", Hierarchy.AnonLocation(com.reportgrid.analytics.Path("usa")))
      )

      forall(expectedMeans(events, "recipientCount", keysf(granularity))) {
        case (eventName, means) =>
          val expected: Map[String, Double] = means.map{ case (k, v) => (k(0), v) }.toMap

          engine.getVariableSeries(TestToken, "/test", Variable(JPath(eventName) \ "recipientCount"), queryTerms) must whenDelivered {
            beLike { 
              case result => 
                val remapped: Map[String, Double] = result.flatMap{ case (k, v) => v.mean.map((k \ "timestamp").deserialize[Instant].toString -> _) }.toMap 
                remapped must_== expected
            }
          }
      }
    }

    "retrieve a time series for occurrences of a value of a variable" in sampleData { sampleEvents =>
      //skip("disabled")
      val granularity = Minute
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)

      val queryTerms = List[TagTerm](
        IntervalTerm(AggregationEngine.timeSeriesEncoding, granularity, TimeSpan(minDate, maxDate)),
        HierarchyLocationTerm("location", Hierarchy.AnonLocation(com.reportgrid.analytics.Path("usa")))
      )

      forallWhen(valueCounts(events)) {
        case ((jpath, value), count) if !jpath.endsInInfiniteValueSpace =>
          val observation = JointObservation(HasValue(Variable(jpath), value))

          engine.getObservationSeries(TestToken, "/test", observation, queryTerms) must whenDelivered {
            beLike { case results => 
              (results.total must_== count.toLong) and
              (results must haveSize((granularity.period(minDate) until maxDate).size))
            }
          }
      }
    }

    "retrieve a time series for occurrences of a value of a variable via the raw events" in sampleData { sampleEvents =>
      ////skip("disabled")
      val granularity = Minute
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)
      val expectedTotals = valueCounts(events) 

      val queryTerms = List[TagTerm](
        IntervalTerm(AggregationEngine.timeSeriesEncoding, granularity, TimeSpan(minDate, maxDate)),
        HierarchyLocationTerm("location", Hierarchy.NamedLocation("country", com.reportgrid.analytics.Path("usa")))
      )

      forallWhen(expectedTotals) {
        case ((jpath, value), count) if !jpath.endsInInfiniteValueSpace => 
          val observation = JointObservation(HasValue(Variable(jpath), value))

          engine.getRawEvents(TestToken, "/test", observation, queryTerms).map(AggregationEngine.countByTerms(_, queryTerms)) must whenDelivered {
            beLike { case results => 
              (results.total must_== count.toLong) and 
              (results must haveSize((granularity.period(minDate) until maxDate).size))
            }
          }
      }
    }

    "count observations of a given value" in sampleData { sampleEvents =>
      //skip("disabled")
      val variables = Variable(".tweeted.retweet") :: Variable(".tweeted.recipientCount") :: Nil

      val queryTerms = List[TagTerm](
        HierarchyLocationTerm("location", Hierarchy.NamedLocation("country", com.reportgrid.analytics.Path("usa")))
      )

      val expectedCounts: Map[List[JValue], CountType] = sampleEvents.foldLeft(Map.empty[List[JValue], CountType]) {
        case (map, Event("tweeted", data, _)) =>
          val values = variables.map(v => data.value(JPath(v.name.nodes.drop(1))))
          map + (values -> map.get(values).map(_ + 1L).getOrElse(1L))

        case (map, _) => map
      }

      forall(expectedCounts) {
        case (values, count) =>
          val observation = JointObservation((variables zip values).map((HasValue(_, _)).tupled).toSet)
          forall(observation.obs) { hasValue => 
            engine.getObservationCount(TestToken, "/test", JointObservation(hasValue), queryTerms) must whenDelivered[CountType] {
              beGreaterThanOrEqualTo(count)
            }
          } and {
            engine.getObservationCount(TestToken, "/test", observation, queryTerms) must whenDelivered[CountType] {
              be_== (count)
            }
          }

      }
    }

    "count observations of a given value in a restricted time slice" in sampleData { sampleEvents =>
      //skip("disabled")
      val granularity = Minute
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)
      val queryTerms = List[TagTerm](
        SpanTerm(AggregationEngine.timeSeriesEncoding, TimeSpan(minDate, maxDate)),
        HierarchyLocationTerm("location", Hierarchy.NamedLocation("country", com.reportgrid.analytics.Path("usa")))
      )

      val variables = Variable(".tweeted.retweet") :: Variable(".tweeted.recipientCount") :: Nil

      val expectedCounts: Map[List[JValue], Long] = events.foldLeft(Map.empty[List[JValue], Long]) {
        case (map, Event("tweeted", data, _)) =>
          val values = variables.map(v => data.value(JPath(v.name.nodes.drop(1))))
          map + (values -> map.get(values).map(_ + 1L).getOrElse(1L))

        case (map, _) => map
      }

      forall(expectedCounts) {
        case (values, count) =>
          val observation = JointObservation((variables zip values).map((HasValue(_, _)).tupled).toSet)
          engine.getObservationCount(TestToken, "/test", observation, queryTerms) must whenDelivered (beEqualTo(count))
      }
    }

    "retrieve intersection counts" in sampleData { sampleEvents =>
      //skip("disabled")
      val variables   = Variable(".tweeted.retweet") :: Variable(".tweeted.recipientCount") :: Nil
      val descriptors = variables.map(v => VariableDescriptor(v, 10, SortOrder.Descending))

      val queryTerms = List[TagTerm](
        HierarchyLocationTerm("location", Hierarchy.NamedLocation("country", com.reportgrid.analytics.Path("usa")))
      )

      val expectedCounts = sampleEvents.foldLeft(Map.empty[List[JValue], Int]) {
        case (map, Event("tweeted", data, _)) =>
          val values = variables.map(v => data.value(JPath(v.name.nodes.drop(1))))

          map + (values -> map.get(values).map(_ + 1).getOrElse(1))

        case (map, _) => map
      }

      engine.getIntersectionCount(TestToken, "/test", descriptors, queryTerms) must whenDelivered {
        beLike { case result => 
          result.collect{ case (JArray(keys), v) if v != 0 => (keys, v) }.toMap must_== expectedCounts
        }
      }
    }

    "retrieve intersection counts for a slice of time" in sampleData { sampleEvents =>
      //skip("disabled")
      val granularity = Minute
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)
      val queryTerms = List[TagTerm](
        IntervalTerm(AggregationEngine.timeSeriesEncoding, granularity, TimeSpan(minDate, maxDate)),
        HierarchyLocationTerm("location", Hierarchy.NamedLocation("country", com.reportgrid.analytics.Path("usa")))
      )

      val variables   = Variable(".tweeted.retweet") :: Variable(".tweeted.recipientCount") :: Nil
      val descriptors = variables.map(v => VariableDescriptor(v, 10, SortOrder.Descending))

      val expectedCounts = events.foldLeft(Map.empty[List[JValue], Int]) {
        case (map, Event("tweeted", data, _)) =>
          val values = variables.map(v => data.value(JPath(v.name.nodes.drop(1))))

          map + (values -> map.get(values).map(_ + 1).getOrElse(1))

        case (map, _) => map
      }

      engine.getIntersectionCount(TestToken, "/test", descriptors, queryTerms).
      map(_.collect{ case (JArray(keys), v) if v != 0 => (keys, v) }.toMap) must {
        whenDelivered[Map[List[JValue], CountType]]( be_== (expectedCounts) )(FutureTimeouts(5, toDuration(3000).milliseconds))
      }
    }

    "retrieve intersection series for a slice of time" in sampleData { sampleEvents =>
      //skip("disabled")
      val granularity = Minute
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)
      val queryTerms = List[TagTerm](
        IntervalTerm(AggregationEngine.timeSeriesEncoding, granularity, TimeSpan(minDate, maxDate)),
        HierarchyLocationTerm("location", Hierarchy.NamedLocation("country", com.reportgrid.analytics.Path("usa")))
      )

      val variables   = Variable(".tweeted.retweet") :: Variable(".tweeted.recipientCount") :: Variable(".tweeted.twitterClient") :: Nil
      val descriptors = variables.map(v => VariableDescriptor(v, 10, SortOrder.Descending))

      val expectedCounts = events.foldLeft(Map.empty[List[JValue], Int]) {
        case (map, Event("tweeted", data, _)) =>
          val values = variables.map(v => data.value(JPath(v.name.nodes.drop(1))))
          map + (values -> map.get(values).map(_ + 1).getOrElse(1))

        case (map, _) => map
      }

      engine.getIntersectionSeries(TestToken, "/test", descriptors, queryTerms) must {
        whenDelivered[ResultSet[JArray, ResultSet[JObject, CountType]]] ({
          beLike { 
            case results => 
              // all returned results must have the same length of time series
              val sizes = results.map(_._2).map(_.size).filter(_ > 0)

              (forall(sizes.zip(sizes.tail)) { case (a, b) => a must_== b }) and
              (results.map(((_: ResultSet[JObject, CountType]).total).second).collect{ case (JArray(keys), v) if v != 0 => (keys, v) }.toMap must_== expectedCounts)
          }
        })(FutureTimeouts(10, toDuration(6000).milliseconds))
      }
    }

    "retrieve all values of infinitely-valued variables that co-occurred with an observation" in sampleData { sampleEvents =>
      //skip("disabled")
      val granularity = Minute
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)

      val expectedValues = events.foldLeft(Map.empty[(String, JPath, JValue), Set[JValue]]) {
        case (map, Event(eventName, data, tags)) =>
          data.flattenWithPath.foldLeft(map) {
            case (map, (jpath, value)) =>
              val key = (eventName, jpath, value)
              map + (key -> (map.getOrElse(key, Set.empty[JValue]) + (data.value \ "~tweet")))
          }
      }

      forallWhen(expectedValues) {
        case ((eventName, jpath, jvalue), infiniteValues) if !jpath.endsInInfiniteValueSpace  => 
          engine.findRelatedInfiniteValues(
            TestToken, "/test", 
            JointObservation(HasValue(Variable(JPath(eventName) \ jpath), jvalue)),
            List(SpanTerm(AggregationEngine.timeSeriesEncoding, TimeSpan(minDate, maxDate)))
          ) map {
            _.map(_.value).toSet
          } must whenDelivered {
            beEqualTo(infiniteValues)
          }
      }
    }

    // this test is here because it's testing internals of the analytics service
    // which are not exposed though the analytics api
    //"AnalyticsService.serializeIntersectionResult must not create duplicate information" in {
    //  val granularity = Minute
    //  val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)
    //  val variables   = Variable(".tweeted.retweet") :: Variable(".tweeted.recipientCount") :: Variable(".tweeted.twitterClient") :: Nil
    //  val descriptors = variables.map(v => VariableDescriptor(v, 10, SortOrder.Descending))

    //  def isFullTimeSeries(jobj: JObject): Boolean = {
    //    jobj.fields.foldLeft(true) {
    //      case (cur, JField(_, jobj @ JObject(_))) => cur && isFullTimeSeries(jobj)
    //      case (cur, JField(_, JArray(values))) => 
    //        cur && (values must notBeEmpty) && (values.map{ case JArray(v) => v(0) }.toSet.size must_== values.size)
    //      case _ => false
    //    }
    //  }

    //  engine.getIntersectionSeries(TestToken, "/test", descriptors, granularity, Some(minDate), Some(maxDate)).
    //  map(AnalyticsService.serializeIntersectionResult[TimeSeriesType](_, _.serialize)) must whenDelivered {
    //    beLike {
    //      case v @ JObject(_) => isFullTimeSeries(v)
    //    }
    //  }
    //}
  }

}

/*

class ReaggregationSpec extends AggregationEngineTests with LocalMongo {
  import AggregationEngine._
  import FutureUtils._

  val config = (new Config()) ->- (_.load(mongoConfigFileData))

  val mongo = new RealMongo(config.configMap("mongo")) 

  val database = mongo.database(dbName)

  val engine = get(AggregationEngine(config, Logger.get, database))

  override implicit val defaultFutureTimeouts = FutureTimeouts(40, toDuration(500).milliseconds)

  object TestTokenStorage extends TokenStorage {
    def lookup(tokenId: String) = Future.sync(Some(TestToken))
  }

  "Re-storing aggregated events" should {
    shareVariables()
    
    val sampleEvents: List[Event] = containerOfN[List, Event](10, fullEventGen).sample.get ->- {
      _.foreach { event => 
        engine.store(TestToken, "/test", event.eventName, event.messageData, 1, true)
      }
    }

    "retrieve and re-aggregate data" in {
      database(selectAll.from(MongoCollectionReference("events")).where("reprocess" === true)) foreach {
        _ foreach {
          engine.aggregateFromStorage(TestTokenStorage, _)
        }
      }

      countStoredEvents(sampleEvents, engine)
    }
  }
}
*/
