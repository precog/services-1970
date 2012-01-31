
package com.reportgrid
package analytics

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
//import org.scalacheck._
import org.scalacheck.{Arbitrary, Gen, Prop}
import Gen.{choose,listOfN,value}

import java.net.URLEncoder

class PathSpec extends Specification with ScalaCheck {
  val genPathComponents : Gen[List[String]] = for {
    componentSize <- choose[Int](0, 10)
    components    <- listOfN(componentSize, Arbitrary.arbString.arbitrary)
  } yield {
    components.map(URLEncoder.encode(_, "UTF-8")).filterNot(_.isEmpty)
  }

  // Generates a pair of (proper string, funky string) for testing
  val genPathString : Gen[Pair[String,String]] = for {
    components <- genPathComponents
    slashCount <- choose[Int](1, 5)
  } yield {
    components match {
      case Nil if components.size % 2 == 0 => ("/", "////////")
      case Nil => ("/", "")
      case encoded   => (encoded.mkString("/", "/", "/"), encoded.mkString("/////".take(slashCount)))
    }
  }

  val genPathList : Gen[Pair[String,List[String]]] = for {
    components <- genPathComponents
  } yield {
    components match {
      case Nil if components.size % 2 == 0 => ("/", List("","","",""))
      case Nil => ("/", List())
      case encoded   => (encoded.mkString("/", "/", "/"), encoded)
    }
  }     

  implicit val arbPathString = Arbitrary(genPathString)

  implicit val arbPathList   = Arbitrary(genPathList)

  override def defaultValues = super.defaultValues ++ Map(minTestsOk -> 100000)

  "rollups for a path" should {
    "Properly clean strings when converting to Paths" in {
      check {
        (componentPair : Pair[String,String]) => { 
          componentPair match {
            case (safeString, pathString) => {
              Path(pathString).path must_== safeString
            }
          }
        }
      }
    }

    "Properly clean string lists when converting to Paths" in {
      check {
        (componentPair : Pair[String,List[String]]) => { 
          componentPair match {
            case (safeString, pathParts) => {
              Path(pathParts).path must_== safeString
            }
          }
        }
      }
    }

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
