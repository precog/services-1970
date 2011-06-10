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

import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JPath
import blueeyes.json.JPathImplicits._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.persistence.mongo._
import blueeyes.concurrent.Future

import net.lag.configgy.{Configgy, Config, ConfigMap}
import net.lag.logging.Logger

import org.specs.{Specification, ScalaCheck}
import org.specs.specification.PendingUntilFixed
import org.scalacheck._
import Gen._
import scalaz._
import Scalaz._
import Periodicity._

trait LocalMongo {
  val mongoConfigFileData = """
    mongo {
      database = "analytics"
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
      maximum_capacity = 10000
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
  """
}

class AggregationEngineSpec extends Specification with PendingUntilFixed with ScalaCheck 
with ArbitraryEvent with FutureMatchers with LocalMongo {
  val config = new Config()
  config.load(mongoConfigFileData)

  //val mongo = new RealMongo(config.configMap("mongo")) 
  val mongo = new MockMongo()
  val database = mongo.database("gluecon")
  
  val engine = get(AggregationEngine(config, Logger.get, database))

  override implicit val defaultFutureTimeouts = FutureTimeouts(40, toDuration(1000).milliseconds)

  def valueCounts(l: List[Event]) = l.foldLeft(Map.empty[(String, JPath, JValue), Int]) {
    case (map, Event(JObject(JField(eventName, obj) :: Nil), _)) =>
      obj.flattenWithPath.foldLeft(map) {
        case (map, (path, value)) =>
          val key = (eventName, path, value)
          val oldCount = map.get(key).getOrElse(0)

          map + (key -> (oldCount + 1))
      }
  }

  def slice(l: List[Event]) = {
    val sortedEvents = l.sortBy(_.timestamp)
    val sliceLength = sortedEvents.size / 2
    val (slice, rest) = sortedEvents.drop(sliceLength / 2).splitAt(sliceLength)
    (slice, slice.head.timestamp, rest.head.timestamp)
  }

  "Aggregation engine" should {
    shareVariables()
    val sampleEvents: List[Event] = containerOfN[List, Event](100, eventGen).sample.get

    for (event <- sampleEvents) {
      engine.aggregate(Token.Test, "/gluecon", event.timestamp, event.data, 1)
    }

    "count events" in {
      def countEvents(eventName: String) = sampleEvents.count {
        case Event(JObject(JField(`eventName`, _) :: Nil), _) => true
        case _ => false
      }

      val eventCounts = EventTypes.map(eventName => (eventName, countEvents(eventName))).toMap

      eventCounts.foreach {
        case (eventName, count) =>
          engine.getVariableCount(Token.Test, "/gluecon/", Variable("." + eventName)) must whenDelivered {
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
          engine.getValues(Token.Test, "/gluecon", Variable(JPath(eventName) \ path)) must whenDelivered {
            haveSameElementsAs(values)
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
          engine.getValues(Token.Test, "/gluecon", Variable(JPath(eventName) \ path)) must whenDelivered {
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

      engine.getHistogramTop(Token.Test, "/gluecon", Variable(".tweeted.retweet"), 10) must whenDelivered {
        haveTheSameElementsAs(retweetCounts)
      }
    }

    "retrieve totals" in {
      val expectedTotals = valueCounts(sampleEvents)

      expectedTotals.foreach {
        case ((eventName, path, value), count) =>
          val variable = Variable(JPath(eventName) \ path) 

          engine.getValueCount(Token.Test, "/gluecon", variable, value) must whenDelivered {
            beEqualTo(count.toLong)
          }
      }
    }

    "retrieve a time series for occurrences of a variable" in {
      val (events, minDate, maxDate) = slice(sampleEvents)

      val expectedTotals = events.foldLeft(Map.empty[(String, JPath), Int]) {
        case (map, Event(JObject(JField(eventName, obj) :: Nil), _)) =>
          obj.flattenWithPath.foldLeft(map) {
            case (map, (path, _)) =>
              val key = (eventName, path)
              map + (key -> (map.get(key).getOrElse(0) + 1))
          }
      }

      expectedTotals.foreach {
        case ((eventName, path), count) =>
          val variable = Variable(JPath(eventName) \ path) 
          engine.getVariableSeries(
            Token.Test, "/gluecon", variable, Minute, Some(minDate), Some(maxDate)
          ) map (_.total(Minute)) must whenDelivered {
            beEqualTo(count.toLong)
          }
      }
    }

    "retrieve a time series for occurrences of a value of a variable" in {      
      val (events, minDate, maxDate) = slice(sampleEvents)
      val expectedTotals = valueCounts(events)
      
      expectedTotals.foreach {
        case ((eventName, path, value), count) =>
          val variable = Variable(JPath(eventName) \ path) 
          engine.getValueSeries(
            Token.Test, "/gluecon", variable, value, Minute, Some(minDate), Some(maxDate)
          ) map (_.total(Minute)) must whenDelivered {
            beEqualTo(count.toLong)
          }
      }
    }

    "retrieve intersection results" in {
      val variables = Variable(".tweeted.retweet") :: Variable(".tweeted.recipientCount") :: Nil

      val expectedCounts = sampleEvents.foldLeft(Map.empty[List[JValue], Int]) {
        case (map, Event(JObject(JField("tweeted", obj) :: Nil), _)) =>
          val values = variables.map(v => obj(JPath(v.name.nodes.drop(1))))

          map + (values -> map.get(values).map(_ + 1).getOrElse(1))

        case (map, _) => map
      }

      expectedCounts.map {
        case (values, count) =>
          val observation = variables.zip(values.map(v => HasValue(v))).toSet

          engine.searchCount(Token.Test, "/gluecon", observation) must whenDelivered (beEqualTo(count))
      }
    }

    "retrieve intersection results" in {      
      val variables   = Variable(".tweeted.retweet") :: Variable(".tweeted.recipientCount") :: Nil
      val descriptors = variables.map(v => VariableDescriptor(v, 10, SortOrder.Descending))

      val expectedCounts = sampleEvents.foldLeft(Map.empty[List[JValue], Int]) {
        case (map, Event(JObject(JField("tweeted", obj) :: Nil), _)) =>
          val values = variables.map(v => obj(JPath(v.name.nodes.drop(1))))

          map + (values -> map.get(values).map(_ + 1).getOrElse(1))

        case (map, _) => map
      }

      engine.intersectCount(Token.Test, "/gluecon", descriptors) must whenDelivered (beEqualTo(expectedCounts)) 
    }

  }

  private def get[A](f: Future[A]): A = {
    val latch = new java.util.concurrent.CountDownLatch(1)

    f.deliverTo(_ => latch.countDown())

    latch.await()

    f.value.get
  }
}
