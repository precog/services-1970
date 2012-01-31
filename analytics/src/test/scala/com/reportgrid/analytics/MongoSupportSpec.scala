package com.reportgrid.analytics
package persistence

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

import blueeyes.json.{ArbitraryJValue, JsonAST, Printer}
import JsonAST._
import Printer.{compact, render}

import MongoSupport.{escapeEventBody,unescapeEventBody}

class MongoSupportSpec extends Specification with ScalaCheck with ArbitraryJValue {
  override def defaultValues = super.defaultValues + (minTestsOk -> 5000)

  "MongoSupport" should {
    "Properly escape dotted field names in JObjects" in {
      check { 
        (jv : JObject) => {
          def ensureNoDots(j : JValue) : Boolean = j match {
            case JField(name, value) => (! name.contains('.')) && ensureNoDots(value)
            case JArray(values)      => values.forall(ensureNoDots)
            case JObject(fields)     => fields.forall(ensureNoDots)
            case _                   => true
          }

          ensureNoDots(escapeEventBody(jv)) must_== true
        }
      }
    }

    "Properly round-trip escaping JObjects" in {
      check { (jv : JObject) => jv must_== unescapeEventBody(escapeEventBody(jv)) }
    }
  }
}
