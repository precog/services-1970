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

import net.lag.configgy.{Configgy, ConfigMap}

import org.specs._
import org.specs.specification.PendingUntilFixed
//import org.specs.util.TimeConversions._
import org.scalacheck._
import Gen._

class AnalyticsServiceSpec extends BlueEyesServiceSpecification with PendingUntilFixed with ScalaCheck 
with AnalyticsService with ArbitraryEvent with FutureMatchers with LocalMongo {
  override def mongoFactory(config: ConfigMap): Mongo = new RealMongo(config)
  //override def mongoFactory(config: ConfigMap): Mongo = new MockMongo()
  override val configuration = "services{analytics{v0{" + mongoConfigFileData + "}}}"

  lazy val analytics = service.contentType[JValue](application/(MimeTypes.json)).query("tokenId", Token.Test.tokenId)

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(10, 1000L.milliseconds)

  "Demo Service" should {
    shareVariables()
    "create events" in {
      val sampleEvents: List[Event] = containerOfN[List, Event](100, eventGen).sample.get

      lazy val tweetedCount = sampleEvents.count {
        case Event(JObject(JField("tweeted", _) :: Nil), _) => true
        case _ => false
      }

      for (event <- sampleEvents) {
        analytics.post[JValue]("/vfs/gluecon")(event.message)
      }

      (analytics.get[JValue]("/vfs/gluecon/.tweeted/count")) must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(result), _) => result.deserialize[Long] must_== tweetedCount
        }
      } 
    }

 //   "calculate intersections" in {
 //     val sampleEvents: List[Event] = containerOfN[List, Event](100, eventGen).sample.get
 //     
 //     analytics.post[JValue]("/intersect") {
 //       JObject(List(
 //         JField("select",     JString("count")),
 //         JField("from",       JString("/vfs/gluecon/")),
 //         JField("properties", JArray(List(
 //           VariableDescriptor(Variable(".tweeted.retweet"), 10, SortOrder.Descending).serialize,
 //           VariableDescriptor(Variable(".tweeted.recipientCount"), 10, SortOrder.Descending).serialize
 //         )))
 //       ))
 //     } must whenDelivered {
 //       beLike {
 //         case HttpResponse(status, _, Some(result), _) =>
 //           println((status, renderNormalized(result)))
 //           fail
 //       }
 //     }
 //   }
  }
}
