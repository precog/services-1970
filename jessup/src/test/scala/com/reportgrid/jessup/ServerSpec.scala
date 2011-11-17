package com.reportgrid
package jessup

import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.core.data.{ByteChunk, BijectionsChunkJson}
import blueeyes.json.JsonAST._

import org.specs2.{json => specs2json, _}
import org.scalacheck._

import scala.collection.mutable

object ServerSpec extends BlueEyesServiceSpecification with Service with ScalaCheck with InMemoryGeoIPComponent with BijectionsChunkJson {
  import Prop._
  import Arbitrary.arbitrary
  
//  noDetailedDiffs()

  def buildGeoIPComponent(databasePath: String) = this
  
  "GeoIP Service" should {
    "retrieve location by ip" ! check { (ip: IPv4, loc: Location) =>
      //skip("There seem to be some JSON encoding issues in BlueEyes that break this test...")
      val ipStr = ip.toString
      
      memory += (ipStr -> loc)
      
      val result = service.contentType[JValue](application/json).get("/" + ipStr)
      result.value must eventually(beSome)
      
      memory -= ipStr        // clean up just to avoid unnecessary memory bloat
      
      val response = result.value.get
      response.status mustEqual HttpStatus(OK)
      response.content must beSome
      
      val jobj = ChunkToJValue(response.content.get)
      
      (jobj \ "country-code") mustEqual JString(loc.countryCode)
      (jobj \ "country-name") mustEqual JString(loc.countryName)
      (jobj \ "region") mustEqual JString(loc.region)
      (jobj \ "city") mustEqual JString(loc.city)
      (jobj \ "postal-code") mustEqual JString(loc.postalCode)
      
      (jobj \ "latitude") mustEqual JDouble(loc.latitude)
      (jobj \ "longitude") mustEqual JDouble(loc.longitude)
      
      (jobj \ "dma-code") mustEqual JInt(loc.dmaCode)
      (jobj \ "area-code") mustEqual JInt(loc.areaCode)
      (jobj \ "metro-code") mustEqual JInt(loc.metroCode)
    }
    
    "fail on non-existent ip" ! check { (ip: IPv4) =>
      val ipStr = ip.toString
        
      memory -= ipStr
        
      val result = service.contentType[JValue](application/json).get("/" + ipStr)
      result.value must eventually(beSome)
      result.value.get.content must beNone
    }
  }
  
  implicit val arbLocation: Arbitrary[Location] = Arbitrary(genLocation)
  
  implicit val arbIp: Arbitrary[IPv4] = Arbitrary(genIp)
  
  import Gen._

  val genIp = for {
    a <- choose(0, 255)
    b <- choose(0, 255)
    c <- choose(0, 255)
    d <- choose(0, 255)
  } yield IPv4(a, b, c, d)
  
  val genLocation: Gen[Location] = for {
    countryCode <- identifier
    countryName <- identifier
    region <- identifier
    city <- identifier
    postalCode <- identifier
    
    latitude <- arbitrary[Float]
    longitude <- arbitrary[Float]
    
    dmaCode <- arbitrary[Int]
    areaCode <- arbitrary[Int]
    metroCode <- arbitrary[Int]
  } yield Location(countryCode, countryName, region, city, postalCode, latitude, longitude, dmaCode, areaCode, metroCode)
  
  case class IPv4(a: Int, b: Int, c: Int, d: Int) {
    override def toString = "%d.%d.%d.%d".format(a, b, c, d)
  }
}

trait InMemoryGeoIPComponent extends GeoIPComponent {
  val memory = mutable.Map[String, Location]()
  
  lazy val GeoIP = new GeoIP {
    def lookup(ip: String) = memory get ip
  }
}
