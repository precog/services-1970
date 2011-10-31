package com.reportgrid.analytics

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.Printer._
import org.specs._
import org.scalacheck._
import Prop._
import SignatureGen._

class SignatureSpec extends Specification with ScalaCheck with ArbitraryJValue {
  implicit val hashFunction = com.reportgrid.common.Sha1HashFunction 

  override val defaultPrettyParams = Pretty.Params(6)

  "generation of a signature" should {
    "for a jvalue" >> {
      "be invariant for order of fields in contained jobjects" in {
        val prop = forAll((obj: JObject) => {
          JObject(scala.util.Random.shuffle(obj.fields)).sig.hashSignature must_== obj.sig.hashSignature
        })
        
        prop must pass
      }

      "mismatch on mismatched objects" in {
        val prop = forAll((v1: JValue, v2: JValue) => (v1 != v2) ==> {
          v1.sig.hashSignature != v2.sig.hashSignature
        })

        prop must pass
      }
    }
  }
}


// vim: set ts=4 sw=4 et:
