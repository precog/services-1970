package com.reportgrid.analytics

import blueeyes._
import blueeyes.core.data.Bijection.identity
import blueeyes.core.http.{HttpResponse, MimeTypes}
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.concurrent.test._
import blueeyes.util.metrics.Duration._

import MimeTypes._

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.xschema.JodaSerializationImplicits._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.JPathImplicits._
import blueeyes.persistence.mongo.{Mongo, RealMongo, MockMongo}

import org.joda.time._
import net.lag.configgy.ConfigMap

import org.specs._
import org.scalacheck.Gen._
import scalaz.Scalaz._

import rosetta.json.blueeyes._

import Periodicity._
import AggregationEngine.ResultSet
import persistence.MongoSupport._
import com.reportgrid.ct._
import com.reportgrid.ct.Mult._
import com.reportgrid.ct.Mult.MDouble._

trait TestAnalyticsService extends BlueEyesServiceSpecification with AnalyticsService with LocalMongo {
  override val configuration = "services{analytics{v0{" + mongoConfigFileData + "}}}"

  //override def mongoFactory(config: ConfigMap): Mongo = new RealMongo(config)
  override def mongoFactory(config: ConfigMap): Mongo = new MockMongo()

  def auditClient(config: ConfigMap) = external.NoopTrackingClient
  def yggdrasil(configMap: ConfigMap) = external.Yggdrasil.Noop[JValue]
  def jessup(configMap: ConfigMap) = external.Jessup.Noop

  lazy val jsonTestService = service.contentType[JValue](application/(MimeTypes.json)).
                                     query("tokenId", Token.Test.tokenId)

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(40, 1000L.milliseconds)
}

class AnalyticsServiceSpec extends TestAnalyticsService with ArbitraryEvent with FutureMatchers {
  "Analytics Service" should {
    shareVariables()

    val sampleEvents: List[Event] = containerOfN[List, Event](10, fullEventGen).sample.get ->- {
      _.foreach(event => jsonTestService.post[JValue]("/vfs/test")(event.message))
    }

    "explore variables" in {
      //skip("disabled")
      (jsonTestService.get[JValue]("/vfs/test/.tweeted")) must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(result), _) => 
            val expected = List(".startup",".retweet",".otherStartups",".~tweet",".location",".twitterClient",".gender",".recipientCount")
            result.deserialize[List[String]] must haveTheSameElementsAs(expected)
        }
      } 
    }

    "count created events" in {
      //skip("disabled")
      lazy val tweetedCount = sampleEvents.count {
        case Event("tweeted", _, _) => true
        case _ => false
      }

      val queryTerms = JObject(
        JField("location", "usa") :: Nil
      )

      (jsonTestService.post[JValue]("/vfs/test/.tweeted/count")(queryTerms)) must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(result), _) => result.deserialize[Long] must_== tweetedCount
        }
      } 
    }

    "count events by get" in {
      //skip("disabled")
      lazy val tweetedCount = sampleEvents.count {
        case Event("tweeted", _, _) => true
        case _ => false
      }

      val queryTerms = JObject(
        JField("location", "usa") :: Nil
      )

      jsonTestService.get[JValue]("/vfs/test/.tweeted/count?location=usa") must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(result), _) => result.deserialize[Long] must_== tweetedCount
        }
      } 
    }

    "return variable series means" in {
      //skip("disabled")
      val (events, minDate, maxDate) = timeSlice(sampleEvents, Hour)
      val expected = expectedMeans(events, "recipientCount", keysf(Hour))

      val queryTerms = JObject(
        JField("start", minDate.getMillis) ::
        JField("end", maxDate.getMillis) ::
        JField("location", "usa") :: Nil
      )

      (jsonTestService.post[JValue]("/vfs/test/.tweeted.recipientCount/series/hour/means")(queryTerms)) must whenDelivered {
        verify {
          case HttpResponse(status, _, Some(contents), _) => 
            val resultData = contents match {
              case JArray(values) => values.flatMap { 
                case JArray(List(JObject(List(JField("timestamp", k), JField("location", k2))), JDouble(v))) => Some((List(k.deserialize[Instant].toString, k2.deserialize[String]), v))
                case JArray(List(JObject(List(_, _)), JNull)) => None
              }
            }

            resultData.toMap must haveTheSameElementsAs(expected("tweeted"))
        }
      } 
    }

    "return variable value series counts" in {
      //skip("disabled")
      val granularity = Hour
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)
      val expectedTotals = valueCounts(events)

      val queryTerms = JObject(
        JField("start", minDate.getMillis) ::
        JField("end", maxDate.getMillis) ::
        JField("location", "usa") :: Nil
      )

      val ((jpath, value), count) = (expectedTotals.find{ case ((k, _), _) => k.nodes.last == JPathField("gender") }).get

      val vtext = compact(render(value))
      val servicePath = "/vfs/test/"+jpath+"/values/"+vtext+"/series/hour"
      if (!jpath.endsInInfiniteValueSpace) {
        (jsonTestService.post[JValue](servicePath)(queryTerms)) must whenDelivered {
          verify {
            case HttpResponse(status, _, Some(JArray(values)), _) => (values must notBeEmpty) //&& (series must_== expected)
          }
        }
      }
    }

    "group variable value series counts" in {
      val granularity = Hour
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)
      val expectedTotals = valueCounts(events)

      val queryTerms = JObject(
        JField("start", minDate.getMillis) ::
        JField("end", maxDate.getMillis) ::
        JField("location", "usa") :: Nil
      )

      val ((jpath, value), count) = (expectedTotals.find{ case ((k, _), _) => k.nodes.last == JPathField("gender") }).get

      val vtext = compact(render(value))
      val servicePath = "/vfs/test/"+jpath+"/values/"+vtext+"/series/hour?groupBy=day"
      if (!jpath.endsInInfiniteValueSpace) {
        (jsonTestService.post[JValue](servicePath)(queryTerms)) must whenDelivered {
          verify {
            case HttpResponse(status, _, Some(JArray(values)), _) => (values must notBeEmpty) //&& (series must_== expected)
          }
        }
      }
    }
  }
}

class AnalyticsServiceCompanionSpec extends Specification {
  import org.joda.time._
  import scalaz._
  import AnalyticsService._

  val startTime = new DateTime(DateTimeZone.UTC)
  val endTime = startTime.plusHours(3)

  "dateTimeZone parsing" should {
    "correctly handle integral zones" in {
      dateTimeZone("1") must_== Success(DateTimeZone.forOffsetHours(1))
      dateTimeZone("12") must_== Success(DateTimeZone.forOffsetHours(12))
    }

    "correctly handle fractional zones" in {
      dateTimeZone("1.5") must_== Success(DateTimeZone.forOffsetHoursMinutes(1, 30))
      dateTimeZone("-1.5") must_== Success(DateTimeZone.forOffsetHoursMinutes(-1, 30))
    }

    "correctly handle named zones" in {
      dateTimeZone("America/Montreal") must_== Success(DateTimeZone.forID("America/Montreal"))
    }
  }

  "time span extraction" should {
    "correctly handle UTC time ranges in parameters" in {
      val parameters = Map(
        'start -> startTime.getMillis.toString, 
        'end ->   endTime.getMillis.toString
      )

      timeSpan(parameters, None) must beLike {
        case Some(Success(TimeSpan(start, end))) => 
          (start must_== startTime.toInstant) && 
          (end must_== endTime.toInstant)
      }
    }

    "correctly handle UTC time ranges in the content" in {
      val content = JObject(
        JField("start", startTime.getMillis) ::
        JField("end", endTime.getMillis) :: Nil
      )

      timeSpan(Map.empty, Some(content)) must beLike {
        case Some(Success(TimeSpan(start, end))) => 
          (start must_== startTime.toInstant) && 
          (end must_== endTime.toInstant)
      }
    }

    "correctly handle offset time ranges in parameters" in {
      val zone = DateTimeZone.forOffsetHours(-6)
      val parameters = Map(
        'start -> startTime.withZoneRetainFields(zone).getMillis.toString, 
        'end ->   endTime.withZoneRetainFields(zone).getMillis.toString,
        'timeZone -> "-6.0"
      )

      timeSpan(parameters, None) must beLike {
        case Some(Success(TimeSpan(start, end))) => 
          (start must_== startTime.toInstant) && 
          (end must_== endTime.toInstant)
      }
    }

    "correctly handle offset time ranges in the content" in {
      val zone = DateTimeZone.forOffsetHours(-6)
      val content = JObject(
        JField("start", startTime.withZoneRetainFields(zone).getMillis) ::
        JField("end", endTime.withZoneRetainFields(zone).getMillis) :: Nil
      )

      val parameters = Map('timeZone -> "-6.0")

      timeSpan(parameters, Some(content)) must beLike {
        case Some(Success(TimeSpan(start, end))) => 
          (start must_== startTime.toInstant) && 
          (end must_== endTime.toInstant)
      }
    }
  }

  "shifting a time series" should {
    val baseDate = new DateTime(DateTimeZone.UTC)
    val testResults: ResultSet[JObject, Long] = List(
      (JObject(JField("timestamp", baseDate.toInstant.serialize) :: Nil), 10L),
      (JObject(JField("timestamp", baseDate.plusHours(1).serialize) :: Nil), 20L),
      (JObject(JField("timestamp", baseDate.plusHours(2).serialize) :: Nil), 30L),
      (JObject(JField("timestamp", baseDate.plusHours(3).serialize) :: Nil), 20L)
    )

    "not shift if the time zone is UTC" in {
      val shifted = shiftTimeSeries[Long](Periodicity.Hour, Some(DateTimeZone.UTC)).apply(testResults).toList 
      shifted must_== testResults.tail
    }

    "shift, but not interpolate, hours with even time zone offsets" in {
      val zone = DateTimeZone.forOffsetHours(1)
      val shifted = shiftTimeSeries[Long](Periodicity.Hour, Some(zone)).apply(testResults).toList 
      val expected = testResults.map((shiftTimeField(_: JObject, zone)).first).tail 
      shifted must_== expected
    }

    "interpolate hours with fractional time zone offsets" in {
      val zone = DateTimeZone.forOffsetHoursMinutes(1, 30)
      val shifted = shiftTimeSeries[Long](Periodicity.Hour, Some(zone)).apply(testResults).toList 
      val expected = List(
        (JObject(JField("datetime", baseDate.plusHours(1).withZone(zone).toString) :: Nil), 15L),
        (JObject(JField("datetime", baseDate.plusHours(2).withZone(zone).toString) :: Nil), 25L),
        (JObject(JField("datetime", baseDate.plusHours(3).withZone(zone).toString) :: Nil), 25L)
      )

      println(shifted.mkString("\n"))
      println("expected")
      println(expected.mkString("\n"))

      shifted must_== expected
    }
  }
}
