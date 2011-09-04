package com.reportgrid.analytics
import  external._

import blueeyes.BlueEyesServer
import blueeyes.json.JsonAST._
import blueeyes.persistence.mongo.Mongo
import com.reportgrid.api.Server
import com.reportgrid.api.blueeyes.ReportGrid
import java.util.Date
import net.lag.configgy.ConfigMap
import rosetta.json.blueeyes._

object AnalyticsServer extends BlueEyesServer with AnalyticsService {
  def mongoFactory(configMap: ConfigMap): Mongo = {
    new blueeyes.persistence.mongo.RealMongo(configMap)
  }

  def auditClient(config: ConfigMap) = {
    NoopTrackingClient
//    val auditToken = config.getString("token", Token.Audit.tokenId)
//    val environment = config.getString("environment", "production") match {
//      case "production" => Server.Production
//      case _            => Server.Local
//    }
//
//    ReportGrid(auditToken, environment)
  }

  def yggdrasil(configMap: ConfigMap): Yggdrasil[JValue] = {
    new YggdrasilServiceProxy[JValue](
      configMap.getString("host", "api.reportgrid.com"),
      configMap.getInt("port"),
      configMap.getString("path", "/services/yggdrasil/v0"))
  }

  def jessup(configMap: ConfigMap): Jessup = {
    new JessupServiceProxy(
      configMap.getString("host", "api.reportgrid.com"),
      configMap.getInt("port"),
      configMap.getString("path", "/services/jessup/v0"))
  }
}


object TestAnalyticsServer extends BlueEyesServer with AnalyticsService {
  def mongoFactory(config: ConfigMap) = {
    new blueeyes.persistence.mongo.mock.MockMongo()
  }

  def auditClient(config: ConfigMap) = NoopTrackingClient
  def yggdrasil(configMap: ConfigMap) = Yggdrasil.Noop[JValue]
  def jessup(configMap: ConfigMap) = Jessup.Noop
}

// vim: set ts=4 sw=4 et:
