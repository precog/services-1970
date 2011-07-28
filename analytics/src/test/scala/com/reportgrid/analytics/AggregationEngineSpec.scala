package com.reportgrid.analytics

import blueeyes._
import blueeyes.core.data.Bijection.identity
import blueeyes.core.http.{HttpStatus, HttpResponse, MimeTypes}
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.concurrent.test.FutureMatchers
import blueeyes.util._
import blueeyes.util.metrics.Duration
import blueeyes.util.metrics.Duration.toDuration
import MimeTypes._

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JPathImplicits._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.persistence.mongo._
import blueeyes.concurrent.Future

import net.lag.configgy.{Configgy, Config, ConfigMap}
import net.lag.logging.Logger

import org.joda.time.Instant
import org.specs.{Specification, ScalaCheck}
import org.specs.specification.PendingUntilFixed
import org.scalacheck._
import scala.math.Ordered._
import Gen._
import scalaz._
import Scalaz._
import Periodicity._

trait LocalMongo {
  val dbName = "test" + scala.util.Random.nextInt(10000)

  def mongoConfigFileData = """
    mongo {
      database = "%s"
      servers  = ["localhost:27017"]
    }

    variable_series {
      collection = "variable_series"
      time_to_idle_millis = 250
      time_to_live_millis = 500

      initial_capacity = 1000
      maximum_capacity = 10000
    }

    variable_value_series {
      collection = "variable_value_series"

      time_to_idle_millis = 250
      time_to_live_millis = 500

      initial_capacity = 1000
      maximum_capacity = 100000
    }

    variable_values {
      collection = "variable_values"

      time_to_idle_millis = 250
      time_to_live_millis = 500

      initial_capacity = 1000
      maximum_capacity = 10000
    }

    variable_children {
      collection = "variable_children"

      time_to_idle_millis = 250
      time_to_live_millis = 500

      initial_capacity = 1000
      maximum_capacity = 10000
    }

    path_children {
      collection = "path_children"

      time_to_idle_millis = 250
      time_to_live_millis = 500

      initial_capacity = 1000
      maximum_capacity = 10000
    }

    log {
      level   = "debug"
      console = true
    }
  """.format(dbName)
}

object Console {
  import FutureUtils._
  def apply(file: java.io.File): Console = {
    apply((new Config()) ->- (_.loadFile(file.getPath)))
  }

  def apply(config: ConfigMap): Console = {
    val mongoConfig = config.configMap("services.analytics.v0.mongo")
    val mongo = new RealMongo(mongoConfig)
    val database = mongo.database(mongoConfig("database"))
    Console(
      AggregationEngine.forConsole(config, Logger.get, database),
      get(TokenManager(database, "tokens"))
    )
  }
}

case class Console(engine: AggregationEngine, tokenManager: TokenManager)

object FutureUtils {
  def get[A](f: Future[A]): A = {
    val latch = new java.util.concurrent.CountDownLatch(1)

    f.deliverTo(_ => latch.countDown())

    latch.await()

    f.value.get
  }
}

class AggregationEngineSpec extends Specification with PendingUntilFixed 
with ArbitraryEvent with FutureMatchers with LocalMongo {
  import AggregationEngine._
  import FutureUtils._

  val config = (new Config()) ->- (_.load(mongoConfigFileData))

  val mongo = new RealMongo(config.configMap("mongo")) 
  //val mongo = new MockMongo()

  val database = mongo.database(dbName)
  
  val engine = get(AggregationEngine(config, Logger.get, database))

  override implicit val defaultFutureTimeouts = FutureTimeouts(60, toDuration(500).milliseconds)

  def valueCounts(l: List[Event]) = l.foldLeft(Map.empty[(String, JPath, JValue), Int]) {
    case (map, Event(JObject(JField(eventName, obj) :: Nil), _)) =>
      obj.flattenWithPath.foldLeft(map) {
        case (map, (path, value)) =>
          val key = (eventName, path, value)
          map + (key -> (map.getOrElse(key, 0) + 1))
      }
  }

  def timeTag(instant: Instant) = Set(Tag("timestamp", TimeReference(AggregationEngine.timeSeriesEncoding, instant)))

  "Aggregation engine" should {
    shareVariables()

    // using the benchmark token for testing because it has order 3
    val sampleEvents: List[Event] = containerOfN[List, Event](100, eventGen).sample.get ->- {
      _.foreach(event => engine.aggregate(Token.Benchmark, "/test", timeTag(event.timestamp), event.data, 1))
    }

    "retrieve path children" in {
      val children = sampleEvents.map {
        case Event(JObject(JField(eventName, _) :: Nil), _) => "." + eventName
      }.toSet

      engine.getPathChildren(Token.Benchmark, "/test") must whenDelivered {
        haveTheSameElementsAs(children)
      }
    }
 
    "count events" in {
      def countEvents(eventName: String) = sampleEvents.count {
        case Event(JObject(JField(`eventName`, _) :: Nil), _) => true
        case _ => false
      }

      val eventCounts = EventTypes.map(eventName => (eventName, countEvents(eventName))).toMap

      eventCounts.foreach {
        case (eventName, count) =>
          engine.getVariableCount(Token.Benchmark, "/test", Variable("." + eventName)) must whenDelivered {
            beEqualTo(count)
          }
      }
    }
 
    "retrieve values" in {
      val values = sampleEvents.foldLeft(Map.empty[(String, JPath), Set[JValue]]) { 
        case (map, Event(JObject(JField(eventName, obj) :: Nil), _)) =>
          obj.flattenWithPath.foldLeft(map) {
            case (map, (path, value)) => 
              val key = (eventName, path)
              val oldValues = map.getOrElse(key, Set.empty)

              map + (key -> (oldValues + value))
          }

        case (map, _) => map
      }

      values.foreach {
        case ((eventName, path), values) =>
          val jpath = JPath(eventName) \ path
          if (!jpath.endsInInfiniteValueSpace) {
            engine.getValues(Token.Benchmark, "/test", Variable(jpath)) must whenDelivered {
              haveSameElementsAs(values)
            }
          }
      }
    }

    "retrieve all values of arrays" in {
      val arrayValues = sampleEvents.foldLeft(Map.empty[(String, JPath), Set[JValue]]) { 
        case (map, Event(JObject(JField(eventName, obj) :: Nil), _)) =>
          map <+> ((obj.children.collect { case JField(name, JArray(elements)) => ((eventName, JPath(name)), elements.toSet) }).toMap)

        case (map, _) => map
      }

      arrayValues.foreach {
        case ((eventName, path), values) =>
          engine.getValues(Token.Benchmark, "/test", Variable(JPath(eventName) \ path)) must whenDelivered {
            haveSameElementsAs(values)
          }
      }
    }

    "retrieve the top results of a histogram" in {
      val retweetCounts = sampleEvents.foldLeft(Map.empty[JValue, Int]) {
        case (map, Event(JObject(JField("tweeted", obj) :: Nil), _)) => 
          val key = obj(".retweet")
          map + (key -> map.get(key).map(_ + 1).getOrElse(1))

        case (map, _) => map
      }

      engine.getHistogramTop(Token.Benchmark, "/test", Variable(".tweeted.retweet"), 10) must whenDelivered {
        haveTheSameElementsAs(retweetCounts)
      }
    }

    "retrieve totals" in {
      val expectedTotals = valueCounts(sampleEvents)

      expectedTotals.foreach {
        case (subset @ (eventName, path, value), count) =>
          val variable = Variable(JPath(eventName) \ path) 
          if (!variable.name.endsInInfiniteValueSpace) {
            engine.searchCount(Token.Benchmark, "/test", JointObservation(HasValue(variable, value)), TimeSpan.Eternity) must whenDelivered {
              beEqualTo(count.toLong)
            }
          }
      }
    }

    "retrieve a time series for occurrences of a variable" in {
      val granularity = Minute
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)
      val intervalTerm = IntervalTerm(AggregationEngine.timeSeriesEncoding, granularity, TimeSpan(minDate, maxDate))

      val expectedTotals = events.foldLeft(Map.empty[(String, JPath), Int]) {
        case (map, Event(JObject(JField(eventName, obj) :: Nil), _)) =>
          obj.flattenWithPath.foldLeft(map) {
            case (map, (path, _)) =>
              val key = (eventName, path)
              map + (key -> (map.getOrElse(key, 0) + 1))
          }
      }

      expectedTotals.foreach {
        case (subset @ (eventName, path), count) =>
          engine.getVariableSeries(Token.Benchmark, "/test", Variable(JPath(eventName) \ path), intervalTerm).
          map(_.total) must whenDelivered {
            beEqualTo(count.toLong)
          }
      }
    }

    "retrieve a time series of statistics over values of a variable" in {
      val granularity = Hour
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)
      val intervalTerm = IntervalTerm(AggregationEngine.timeSeriesEncoding, granularity, TimeSpan(minDate, maxDate))

      expectedMeans(events, granularity, "recipientCount").foreach {
        case (eventName, means) =>
          engine.getVariableSeries(Token.Benchmark, "/test", Variable(JPath(eventName) \ "recipientCount"), intervalTerm) must whenDelivered {
            verify(_.flatMap(_._2.mean) == means)
          }
      }
    }

    "retrieve a time series for occurrences of a value of a variable" in {      
      val granularity = Minute
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)
      val expectedTotals = valueCounts(events)
      val intervalTerm = IntervalTerm(AggregationEngine.timeSeriesEncoding, granularity, TimeSpan(minDate, maxDate))

      expectedTotals.foreach {
        case (subset @ (eventName, path, value), count) =>
          val variable = Variable(JPath(eventName) \ path)
          if (!variable.name.endsInInfiniteValueSpace) {
            val observation = JointObservation(HasValue(variable, value))

            engine.searchSeries(Token.Benchmark, "/test", observation, intervalTerm) must whenDelivered {
              verify {
                results => 
                  (results.total must_== count.toLong) && 
                  (results must haveSize((granularity.period(minDate) to maxDate).size))
              }
            }
          }
      }
    }

    "count observations of a given value" in {
      val variables = Variable(".tweeted.retweet") :: Variable(".tweeted.recipientCount") :: Nil

      val expectedCounts = sampleEvents.foldLeft(Map.empty[List[JValue], Int]) {
        case (map, Event(JObject(JField("tweeted", obj) :: Nil), _)) =>
          val values = variables.map(v => obj(JPath(v.name.nodes.drop(1))))
          map + (values -> map.get(values).map(_ + 1).getOrElse(1))

        case (map, _) => map
      }

      expectedCounts.map {
        case (values, count) =>
          val observation = JointObservation((variables zip values).map((HasValue(_, _)).tupled).toSet)

          engine.searchCount(Token.Benchmark, "/test", observation, TimeSpan.Eternity) must whenDelivered (beEqualTo(count))
      }
    }

    "count observations of a given value in a restricted time slice" in {
      val granularity = Minute
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)

      val variables = Variable(".tweeted.retweet") :: Variable(".tweeted.recipientCount") :: Nil

      var expectedPeriods = Set.empty[Period]
      val expectedCounts = events.foldLeft(Map.empty[List[JValue], Int]) {
        case (map, Event(JObject(JField("tweeted", obj) :: Nil), time)) =>
          expectedPeriods += (granularity.period(time))
          val values = variables.map(v => obj(JPath(v.name.nodes.drop(1))))
          map + (values -> map.get(values).map(_ + 1).getOrElse(1))

        case (map, _) => map
      }

      expectedCounts.map {
        case (values, count) =>
          val observation = JointObservation((variables zip values).map((HasValue(_, _)).tupled).toSet)

          engine.searchCount(Token.Benchmark, "/test", observation, TimeSpan(minDate, maxDate)) must whenDelivered (beEqualTo(count))
      }
    }

    "retrieve intersection counts" in {      
      val variables   = Variable(".tweeted.retweet") :: Variable(".tweeted.recipientCount") :: Nil
      val descriptors = variables.map(v => VariableDescriptor(v, 10, SortOrder.Descending))

      val expectedCounts = sampleEvents.foldLeft(Map.empty[List[JValue], Int]) {
        case (map, Event(JObject(JField("tweeted", obj) :: Nil), _)) =>
          val values = variables.map(v => obj(JPath(v.name.nodes.drop(1))))

          map + (values -> map.get(values).map(_ + 1).getOrElse(1))

        case (map, _) => map
      }

      engine.intersectCount(Token.Benchmark, "/test", descriptors, TimeSpan.Eternity) must whenDelivered (beEqualTo(expectedCounts)) 
    }

    "retrieve intersection counts for a slice of time" in {      
      val granularity = Minute
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)
      val expectedTotals = valueCounts(events)

      val variables   = Variable(".tweeted.retweet") :: Variable(".tweeted.recipientCount") :: Nil
      val descriptors = variables.map(v => VariableDescriptor(v, 10, SortOrder.Descending))

      val expectedCounts = events.foldLeft(Map.empty[List[JValue], Int]) {
        case (map, Event(JObject(JField("tweeted", obj) :: Nil), _)) =>
          val values = variables.map(v => obj(JPath(v.name.nodes.drop(1))))

          map + (values -> map.get(values).map(_ + 1).getOrElse(1))

        case (map, _) => map
      }

      engine.intersectCount(Token.Benchmark, "/test", descriptors, TimeSpan(minDate, maxDate)) must whenDelivered (beEqualTo(expectedCounts)) 
    }

    "retrieve intersection series for a slice of time" in {      
      val granularity = Minute
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)
      val intervalTerm = IntervalTerm(AggregationEngine.timeSeriesEncoding, granularity, TimeSpan(minDate, maxDate))
      val expectedTotals = valueCounts(events)

      val variables   = Variable(".tweeted.retweet") :: Variable(".tweeted.recipientCount") :: Variable(".tweeted.twitterClient") :: Nil
      val descriptors = variables.map(v => VariableDescriptor(v, 10, SortOrder.Descending))

      val expectedCounts = events.foldLeft(Map.empty[List[JValue], Int]) {
        case (map, Event(JObject(JField("tweeted", obj) :: Nil), _)) =>
          val values = variables.map(v => obj(JPath(v.name.nodes.drop(1))))
          map + (values -> map.get(values).map(_ + 1).getOrElse(1))

        case (map, _) => map
      }

      engine.intersectSeries(Token.Benchmark, "/test", descriptors, intervalTerm) must whenDelivered {
        verify { results => 
          // all returned results must have the same length of time series
          val sizes = results.values.map(_.size).toList

          (results.values must notBeEmpty) &&
          sizes.zip(sizes.tail).forall { case (a, b) => a must_== b } &&
          (results.mapValues(_.total).filter(_._2 != 0) must haveTheSameElementsAs(expectedCounts))
        }
      }
    }

    "retrieve all values of infinitely-valued variables that co-occurred with an observation" in {
      val granularity = Minute
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)

      val expectedValues = events.foldLeft(Map.empty[(String, JPath, JValue), (Set[JValue], Set[Period])]) {
        case (map, Event(JObject(JField(eventName, obj) :: Nil), time)) =>
          obj.flattenWithPath.foldLeft(map) {
            case (map, (jpath, value)) if !jpath.endsInInfiniteValueSpace =>
              val key = (eventName, jpath, value)
              map + (key -> (map.getOrElse(key, (Set.empty[JValue], Set.empty[Period])).mapElements(_ + (obj \ "~tweet"), _ + granularity.period(time))))

            case (map, _) => map
          }
      }

      expectedValues.foreach {
        case ((eventName, jpath, jvalue), (infiniteValues, periods)) => 
          engine.findRelatedInfiniteValues(
            Token.Benchmark, "/test", 
            JointObservation(HasValue(Variable(JPath(eventName) \ jpath), jvalue)),
            TimeSpan(minDate, maxDate)
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

    //  engine.intersectSeries(Token.Benchmark, "/test", descriptors, granularity, Some(minDate), Some(maxDate)).
    //  map(AnalyticsService.serializeIntersectionResult[TimeSeriesType](_, _.serialize)) must whenDelivered {
    //    beLike {
    //      case v @ JObject(_) => isFullTimeSeries(v)
    //    }
    //  }
    //}
  }
}
