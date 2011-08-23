package com.reportgrid
package jessup

import net.lag.configgy.Config

import blueeyes.BlueEyesServer
import blueeyes.BlueEyesServiceBuilder
import blueeyes.concurrent.Future
import blueeyes.concurrent.Future._

import blueeyes.core.http.HttpHeaders
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.combinators.HttpRequestCombinators
import blueeyes.core.http.MimeTypes._
import blueeyes.core.http.{HttpStatusCodes, HttpStatus, HttpRequest, HttpResponse}
import blueeyes.core.data.{BijectionsChunkJson, ByteChunk}

import blueeyes.json.JsonAST._

trait Server extends BlueEyesServiceBuilder with HttpRequestCombinators with BijectionsChunkJson with GeoIPComponent {
  val geoipService = service("jessup", "1.0") { context =>
    startup {
      Future sync JessupConfig("")
    } ->
    request { config =>
      import config._
      
      path("/'ip") {
        produce(application/json) {
          get { req: HttpRequest[ByteChunk] =>
            Future sync {
              val json = GeoIP.lookup(req.parameters('ip)) map locationToJson
              HttpResponse(content = json map { JValueToChunk(_): ByteChunk })
            }
          }
        }
      }
    } ->
    shutdown { config =>
      Future sync ()
    }
  }
  
  def locationToJson(loc: Location): JObject = {
    val fields = List(
      JField("country-code", loc.countryCode),
      JField("country-name", loc.countryName),
      JField("region", loc.region),
      JField("city", loc.city),
      JField("postal-code", loc.postalCode),
      JField("latitude", loc.latitude),
      JField("longitude", loc.longitude),
      JField("dma-code", loc.dmaCode),
      JField("area-code", loc.areaCode),
      JField("metro-code", loc.metroCode))
    
    JObject(fields)
  }
}

object Server extends BlueEyesServer with Server with MaxMindGeoIPComponent {
  var DatabasePath = "jessup/src/main/resources/GeoLiteCity.dat"      // we should probably set this in main
  
  override val rootConfig = {
    val back = new Config
    back.setConfigMap("server", new Config)
    back
  }
}

case class JessupConfig(dbPath: String)
