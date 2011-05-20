package com.reportgrid.analytics

import blueeyes.core.data.Bijection.identity
import blueeyes.core.http.{HttpStatus, HttpResponse, MimeTypes}
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.concurrent.test.FutureMatchers
import blueeyes.concurrent.Duration
import blueeyes.concurrent.Duration.toDuration
import MimeTypes._

import blueeyes.json.JsonAST.{JValue, JObject, JField, JString, JNothing, JArray}
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

class AggregationEngineSpec extends Specification with PendingUntilFixed with ScalaCheck 
with ArbitraryEvent with FutureMatchers {
  val config = new Config()

  config.load("""
    mongo {
      database = "analytics"
      servers  = ["localhost:27017"]
    }
    variable_series {
      collection = "variable_series"
      time_to_idle_millis = 500
      time_to_live_millis = 1000

      initial_capacity = 1000
      maximum_capacity = 10000
    }

    variable_value_series {
      collection = "variable_value_series"

      time_to_idle_millis = 500
      time_to_live_millis = 1000

      initial_capacity = 1000
      maximum_capacity = 10000
    }

    variable_values {
      collection = "variable_values"

      time_to_idle_millis = 500
      time_to_live_millis = 1000

      initial_capacity = 1000
      maximum_capacity = 10000
    }

    variable_children {
      collection = "variable_children"

      time_to_idle_millis = 500
      time_to_live_millis = 1000

      initial_capacity = 1000
      maximum_capacity = 10000
    }

    path_children {
      collection = "path_children"

      time_to_idle_millis = 500
      time_to_live_millis = 1000

      initial_capacity = 1000
      maximum_capacity = 10000
    }

    log {
      level   = "debug"
      console = true
    }
  """)

  val mongo = new MockMongo()//
  val database = mongo.database("gluecon")
  
  val engine = get(AggregationEngine(config, Logger.get, database))

  override implicit val defaultFutureTimeouts = FutureTimeouts(60, toDuration(1000).milliseconds)

  "Aggregation engine" should {
    shareVariables()
    val sampleEvents: List[Event] = containerOfN[List, Event](100, eventGen).sample.get

    for (event <- sampleEvents) {
      engine.aggregate(Token.Test, "/gluecon", event.timestamp, event.data, 1)
    }

//    "aggregate simple events" in {
//      def countEvents(eventName: String) = sampleEvents.count {
//        case Event(JObject(JField(`eventName`, _) :: Nil), _) => true
//        case _ => false
//      }
//
//      val eventCounts = EventTypes.map(eventName => (eventName, countEvents(eventName))).toMap
//
//      eventCounts.foreach {
//        case (eventName, count) =>
//          engine.getVariableCount(Token.Test, "/gluecon/", Variable("." + eventName)) must whenDelivered {
//            beEqualTo(count)
//          }
//      }
//    }
//
//    "retrieve the top results of a histogram" in {
//      val retweetCounts = sampleEvents.foldLeft(Map.empty[JValue, Int]) {
//        case (map, Event(JObject(JField("tweeted", obj) :: Nil), _)) => 
//          val key = obj(".retweet")
//          map + (key -> map.get(key).map(_ + 1).getOrElse(1))
//
//        case (map, _) => map
//      }
//
//      engine.getHistogramTop(Token.Test, "/gluecon", Variable(".tweeted.retweet"), 10) must whenDelivered {
//        beEqualTo(retweetCounts)
//      }
//    }

    "search for intersection results" in {
      val variables = Variable(".tweeted.retweet") :: Variable(".tweeted.recipientCount") :: Nil

      val expectedCounts = sampleEvents.foldLeft(Map.empty[List[JValue], Int]) {
        case (map, Event(JObject(JField("tweeted", obj) :: Nil), _)) =>
          val values = variables.map(v => obj(JPath(v.name.nodes.drop(1))))

          map + (values -> map.get(values).map(_ + 1).getOrElse(1))

        case (map, _) => map
      }

      //println("Expected: " + expectedCounts)

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

      //println("expected: " + expectedCounts.map(((_:List[JValue]).map(renderNormalized)).first))

      engine.intersectCount(Token.Test, "/gluecon", descriptors) must whenDelivered (beEqualTo(expectedCounts)) /*{
        verify(x => (x ->- {m => println(m.map(((_:List[JValue]).map(renderNormalized)).first))}) must_== expectedCounts)
      }*/
    }
  }

  private def get[A](f: Future[A]): A = {
    val latch = new java.util.concurrent.CountDownLatch(1)

    f.deliverTo(_ => latch.countDown())

    latch.await()

    f.value.get
  }
}
