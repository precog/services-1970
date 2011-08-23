package com.reportgrid
package jessup

import com.maxmind.geoip.LookupService

trait GeoIPComponent {
  def GeoIP: GeoIP
  
  trait GeoIP {
    def lookup(ip: String): Option[Location]
  }
}

trait MaxMindGeoIPComponent extends GeoIPComponent {
  def DatabasePath: String
  
  override lazy val GeoIP = new GeoIP {
    val service = new LookupService(DatabasePath, LookupService.GEOIP_INDEX_CACHE)
    
    def lookup(ip: String) = {
      Option(service.getLocation(ip)) map { result =>
        Location(result.countryCode, result.countryName, result.region, result.city,
          result.postalCode, result.latitude, result.longitude, result.dma_code,
          result.area_code, result.metro_code)
      }
    }
  }
}


case class Location(
  countryCode: String,
  countryName: String,
  region: String,
  city: String,
  postalCode: String,
  
  latitude: Float,
  longitude: Float,
  
  dmaCode: Int,
  areaCode: Int,
  metroCode: Int)
