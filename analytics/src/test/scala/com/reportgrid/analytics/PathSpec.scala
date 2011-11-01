
package com.reportgrid
package analytics

import org.specs.{Specification, ScalaCheck}
import org.scalacheck._
import org.scalacheck.Gen._
import org.scalacheck.Prop._

class PathSpec extends Specification with ScalaCheck {
  "rollups for a path" should {
    "not roll up when flag is false" in {
      val sample = analytics.Path("/my/fancy/path")
      sample.rollups(0) must_== List(sample)
    }

    "include the original path" in {
      val sample = analytics.Path("/my/fancy/path")
      sample.rollups(3) must haveTheSameElementsAs(
        sample :: 
        analytics.Path("/my/fancy") :: 
        analytics.Path("/my") :: 
        analytics.Path("/") :: Nil
      )
    }
    
    "Roll up a limited distance" in {
      val sample = analytics.Path("/my/fancy/path")
      sample.rollups(2) must haveTheSameElementsAs(
        sample :: 
        analytics.Path("/my/fancy") :: 
        analytics.Path("/my") :: Nil
      )
    }
  }
}
