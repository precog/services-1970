package com.reportgrid.analytics

import blueeyes.json._
import blueeyes.json.JsonAST._
import org.specs._
import org.scalacheck._
import Prop._
import SignatureGen._
import Hashable._

class SignatureSpec extends Specification with ScalaCheck with ArbitraryJValue{
  implicit val hashFunction = Sha1HashFunction 

  "generation of a signature" should {
    "for a jvalue" >> {
      "be invariant for order of fields in contained jobjects" in {
        val prop = forAll((obj: JObject) => {
          hashSignature(JObject(scala.util.Random.shuffle(obj.fields))) must_== hashSignature(obj)
        })
        
        prop must pass
      }

      "mismatch on mismatched objects" in {
        val prop = forAll((v1: JValue, v2: JValue) => (v1 != v2) ==> {
          hashSignature(v1) != hashSignature(v2)
        })

        prop must pass
      }
    }
  }
}


// vim: set ts=4 sw=4 et:
