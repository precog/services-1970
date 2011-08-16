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

import blueeyes.json.JsonAST._
import blueeyes.json.xschema.JodaSerializationImplicits._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.JPathImplicits._
import blueeyes.persistence.mongo.{Mongo, RealMongo, MockMongo}

import com.reportgrid.api.ReportGridTrackingClient

import java.util.Date
import net.lag.configgy.{Configgy, ConfigMap}

import org.specs._
import org.scalacheck.Gen._
import scalaz.Scalaz._

import rosetta.json.blueeyes._

import Periodicity._
import persistence.MongoSupport._
import org.joda.time.Instant

class AnalyticsServiceSpec extends BlueEyesServiceSpecification 
with AnalyticsService with ArbitraryEvent with FutureMatchers with LocalMongo {
  override val configuration = "services{analytics{v0{" + mongoConfigFileData + "}}}"

  //override def mongoFactory(config: ConfigMap): Mongo = new RealMongo(config)
  override def mongoFactory(config: ConfigMap): Mongo = new MockMongo()

  override def auditClientFactory(config: ConfigMap) = NoopTrackingClient

  lazy val jsonTestService = service.contentType[JValue](application/(MimeTypes.json)).
                                     query("tokenId", Token.Test.tokenId)

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(40, 1000L.milliseconds)

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

    "return variable series means" in {
      //skip("disabled")
      val (events, minDate, maxDate) = timeSlice(sampleEvents, Hour)
      val expected = expectedMeans(events, Hour, "recipientCount")

      (jsonTestService.header("Range", "time=" + minDate.getMillis + "-" + maxDate.getMillis).get[JValue]("/vfs/test/.tweeted.recipientCount/series/hour/means")) must whenDelivered {
        verify {
          case HttpResponse(status, _, Some(contents), _) => 
            contents match {
              case JArray(values) => values.flatMap { 
                case JArray(List(JObject(List(JField(_, k))), JDouble(v))) => Some((k.deserialize[Instant], v))
                case JArray(List(JObject(List(JField(_, _))), JNull)) => None
              } must haveTheSameElementsAs(expected("tweeted"))
            }
        }
      } 
    }

    "return variable value series counts" in {
      //skip("disabled")
      val (events, minDate, maxDate) = timeSlice(sampleEvents, Hour)
      //val expected = expectedCounts(events, Hour, "gender")
      //expected must notBeEmpty

      (jsonTestService.header("Range", "time=" + minDate.getMillis + "-" + maxDate.getMillis).get[JValue]("/vfs/test/.tweeted.gender/values/\"male\"/series/hour")) must whenDelivered {
        verify {
          case HttpResponse(status, _, Some(contents), _) => 
            contents match {
              case JArray(values) => (values must notBeEmpty) //&& (series must_== expected)
            }
        }
      } 
    }
  }
}
