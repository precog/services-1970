package com.reportgrid.analytics

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.JodaSerializationImplicits._

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.Printer._
import org.specs._
import org.scalacheck._
import Prop._

import scalaz.Scalaz._
import scalaz.Validation

class AnalyticsSerializationSpec extends Specification with ScalaCheck with AnalyticsSerialization{
  "deserializing a limits with a default" should {
    "always return a valid limits" in {
      val default = Limits(1, 1, 1, 1)
      JNothing.deserialize[Limits](limitsExtractor(default)) must_== default
    }

    "use the values of the default when only part of the limits are specified" in {
      val default = Limits(1, 1, 1, 1)
      JObject(JField("order", 3) :: JField("limit", 3) :: Nil).deserialize(limitsExtractor(default)) must_== Limits(3, 3, 1, 1)
    }
  }
}

// vim: set ts=4 sw=4 et:
