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
import blueeyes.core.data.Bijection._

import blueeyes.json.JsonAST._

trait Service extends BlueEyesServiceBuilder with HttpRequestCombinators {
  import BijectionsChunkJson.JValueToChunk

  def buildGeoIPComponent(databasePath: String): GeoIPComponent

  val geoipService = service("jessup", "1.0") { 
    healthMonitor { monitor => context =>
      startup {
        import context._
        Future sync JessupConfig(buildGeoIPComponent(config.getString("dbpath", "/opt/reportgrid/GeoLiteCity.dat")))
      } ->
      request { config =>
        import config._
        
        path("/'ip") {
          produce(application/json) {
            get { req: HttpRequest[ByteChunk] =>
              Future async {
                val json: Option[JValue] = geoipComponent.GeoIP.lookup(req.parameters('ip)) map locationToJson
                HttpResponse(content = json.map(_.as[ByteChunk]))
              }
            }
          }
        }
      } ->
      shutdown { config =>
        Future sync ()
      }
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

object Server extends BlueEyesServer with Service {
  override def buildGeoIPComponent(databasePath: String): GeoIPComponent = new MaxMindGeoIPComponent {
    override val DatabasePath = databasePath
  }
}

case class JessupConfig(geoipComponent: GeoIPComponent)
