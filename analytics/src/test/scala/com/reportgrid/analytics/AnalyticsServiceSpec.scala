package com.reportgrid.analytics

import blueeyes._
import blueeyes.core.data.Bijection.identity
import blueeyes.core.http.{HttpStatus, HttpResponse, MimeTypes}
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.concurrent.test._
import blueeyes.util.metrics.Duration._
import blueeyes.json.JsonDSL._

import MimeTypes._

import blueeyes.json.JsonAST.{JValue, JObject, JField, JString, JNothing, JArray}
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.JPathImplicits._
import blueeyes.persistence.mongo.{Mongo, RealMongo, MockMongo}

import com.reportgrid.api.ReportGridTrackingClient

import java.util.Date
import net.lag.configgy.{Configgy, ConfigMap}

import org.specs._
import org.specs.specification.PendingUntilFixed
import org.scalacheck._

import rosetta.json.blueeyes._

import Gen._

class AnalyticsServiceSpec extends BlueEyesServiceSpecification with PendingUntilFixed with ScalaCheck 
with AnalyticsService with ArbitraryEvent with FutureMatchers with LocalMongo {
  override val configuration = "services{analytics{v0{" + mongoConfigFileData + "}}}"

  //override def mongoFactory(config: ConfigMap): Mongo = new RealMongo(config)
  override def mongoFactory(config: ConfigMap): Mongo = new MockMongo()

  override def auditClientFactory(config: ConfigMap) = new ReportGridTrackingClient[JValue] {
    override def track(path: com.reportgrid.api.Path, name: String, properties: JValue = jsonImplementation.EmptyObject, rollup: Boolean = false, timestamp: Option[Date] = None, count: Option[Int] = None, headers: Map[String, String] = Map.empty): Unit = {
      println("Tracked " + path + "; " + name + " - " + properties)
    }
  }

  lazy val jsonTestService = service.contentType[JValue](application/(MimeTypes.json)).
                                     query("tokenId", Token.Test.tokenId)

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(10, 1000L.milliseconds)

  "Analytics Service" should {
    shareVariables()

    val sampleEvents: List[Event] = containerOfN[List, Event](100, eventGen).sample.get ->- {
      _.foreach(event => jsonTestService.post[JValue]("/vfs/test")(event.message))
    }

    "count created events" in {
      lazy val tweetedCount = sampleEvents.count {
        case Event(JObject(JField("tweeted", _) :: Nil), _) => true
        case _ => false
      }

      (jsonTestService.get[JValue]("/vfs/test/.tweeted/count")) must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(result), _) => result.deserialize[Long] must_== tweetedCount
        }
      } 
    }
  }
}
