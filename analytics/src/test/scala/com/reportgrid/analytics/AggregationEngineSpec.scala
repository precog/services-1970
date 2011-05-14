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
import blueeyes.persistence.mongo.{Mongo, MockMongo}

import net.lag.configgy.{Configgy, Config, ConfigMap}
import net.lag.logging.Logger

import org.specs.{Specification, ScalaCheck}
import org.specs.specification.PendingUntilFixed
import org.scalacheck._
import Gen._

class AggregationEngineSpec extends Specification with PendingUntilFixed with ScalaCheck 
with ArbitraryEvent with FutureMatchers {
  val config = new Config()

  config.load("""
    variable_series {
      collection = "variable_series"
      time_to_idle_millis = 500
      time_to_live_millis = 100

      initial_capacity = 1000
      maximum_capacity = 10000
    }

    variable_value_series {
      collection = "variable_value_series"

      time_to_idle_millis = 500
      time_to_live_millis = 100

      initial_capacity = 1000
      maximum_capacity = 10000
    }

    variable_values {
      collection = "variable_values"

      time_to_idle_millis = 500
      time_to_live_millis = 100

      initial_capacity = 1000
      maximum_capacity = 10000
    }

    variable_children {
      collection = "variable_children"

      time_to_idle_millis = 500
      time_to_live_millis = 100

      initial_capacity = 1000
      maximum_capacity = 10000
    }

    path_children {
      collection = "path_children"

      time_to_idle_millis = 500
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
  
  val engine = new AggregationEngine2(config, Logger.get, database) 

  override implicit val defaultFutureTimeouts = FutureTimeouts(50, toDuration(100).milliseconds)

  "Aggregation engine" should {
    "aggregate simple events" in {
      val sampleEvents: List[Event] = containerOfN[List, Event](2, eventGen).sample.get

      def countEvents(eventName: String) = sampleEvents.count {
        case Event(JObject(JField(eventName, _) :: Nil), _) => true
        case _ => false
      }

      val eventCounts = EventTypes.map(eventName => (eventName, countEvents(eventName))).toMap

      for (event <- sampleEvents) {
        engine.aggregate(Token.Test, "/vfs/gluecon", event.timestamp, event.message, 1)
      }

      eventCounts.foreach {
        case (eventName, count) =>
          engine.getVariableCount(Token.Test, "/vfs/gluecon/", Variable("." + eventName)) must deliver {
            beSome(count)
          }
      }
    }
  }
}
