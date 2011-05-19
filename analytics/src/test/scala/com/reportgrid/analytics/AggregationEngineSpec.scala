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
import blueeyes.json.JPathImplicits._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.persistence.mongo._

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
    variable_series {
      collection = "variable_series"
      time_to_idle_millis = 50
      time_to_live_millis = 100

      initial_capacity = 1000
      maximum_capacity = 10000
    }

    variable_value_series {
      collection = "variable_value_series"

      time_to_idle_millis = 50
      time_to_live_millis = 100

      initial_capacity = 1000
      maximum_capacity = 10000
    }

    variable_values {
      collection = "variable_values"

      time_to_idle_millis = 50
      time_to_live_millis = 100

      initial_capacity = 1000
      maximum_capacity = 10000
    }

    variable_children {
      collection = "variable_children"

      time_to_idle_millis = 50
      time_to_live_millis = 100

      initial_capacity = 1000
      maximum_capacity = 10000
    }

    path_children {
      collection = "path_children"

      time_to_idle_millis = 50
      time_to_live_millis = 100

      initial_capacity = 1000
      maximum_capacity = 10000
    }

    mongo {
      database = "analytics"

      servers = ["mongodb01.reportgrid.com:27017", "mongodb02.reportgrid.com:27017", "mongodb03.reportgrid.com:27017"]
    }

    log {
      level   = "debug"
      console = true
    }
  """)

  val mongo = new MockMongo()
  val database = mongo.database("gluecon")
  
  val engine = new AggregationEngine(config, Logger.get, database) 

  override implicit val defaultFutureTimeouts = FutureTimeouts(60, toDuration(1000).milliseconds)

  "Aggregation engine" should {
    shareVariables()
    val sampleEvents: List[Event] = containerOfN[List, Event](100, eventGen).sample.get

    for (event <- sampleEvents) {
      engine.aggregate(Token.Test, "/vfs/gluecon", event.timestamp, event.data, 1)
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
//          engine.getVariableCount(Token.Test, "/vfs/gluecon/", Variable("." + eventName)) must whenDelivered {
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
//      engine.getHistogramTop(Token.Test, "/vfs/gluecon", Variable(".tweeted.retweet"), 10) must whenDelivered {
//        beEqualTo(retweetCounts)
//      }
//    }
//
    "retrieve intersection results" in {
      val expectedCounts = sampleEvents.foldLeft(Map.empty[List[JValue], Int]) {
        case (map, Event(JObject(List(JField(_, obj))), _)) =>
          val key = List(obj(".retweet"), obj(".recipientCount"))
          map + (key -> map.get(key).map(_ + 1).getOrElse(1))
      }

      println("expected: " + expectedCounts.map(((_:List[JValue]).map(renderNormalized)).first))

      engine.intersectCount(
        Token.Test, "/vfs/gluecon", 
        List(
          VariableDescriptor(Variable(".tweeted.retweet"), 10, SortOrder.Descending),
          VariableDescriptor(Variable(".tweeted.recipientCount"), 10, SortOrder.Descending)
        ),
        None, None
      ) must whenDelivered {
        verify(x => (x ->- {m => println(m.map(((_:List[JValue]).map(renderNormalized)).first))}) must_== expectedCounts)
      }
    }
  }
}
