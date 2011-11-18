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

import org.specs2.mutable.Specification
import org.scalacheck.Gen._
import scalaz.Scalaz._

import rosetta.json.blueeyes._

import Periodicity._
import AggregationEngine.ResultSet
import persistence.MongoSupport._
import com.reportgrid.ct._
import com.reportgrid.ct.Mult._
import com.reportgrid.ct.Mult.MDouble._


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
          (start must_== startTime.toInstant) and
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
          (start must_== startTime.toInstant) and
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
          (start must_== startTime.toInstant) and
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
          (start must_== startTime.toInstant) and
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

      shifted must_== expected
    }
  }
}

// vim: set ts=4 sw=4 et:
