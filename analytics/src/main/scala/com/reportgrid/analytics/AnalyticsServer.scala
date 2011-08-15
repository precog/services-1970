package com.reportgrid.analytics

import blueeyes.BlueEyesServer
import blueeyes.json.JsonAST.{JValue, JObject, JField, JString, JNothing, JArray}
import blueeyes.persistence.mongo.Mongo
import com.reportgrid.api.ReportGridTrackingClient
import com.reportgrid.api.Server
import com.reportgrid.api.blueeyes.ReportGrid
import java.util.Date
import net.lag.configgy.ConfigMap
import rosetta.json.blueeyes._

object AnalyticsServer extends BlueEyesServer with AnalyticsService {
  def mongoFactory(configMap: ConfigMap): Mongo = {
    new blueeyes.persistence.mongo.RealMongo(configMap)
  }

  def auditClientFactory(config: ConfigMap) = {
    NoopTrackingClient
//    val auditToken = config.getString("token", Token.Audit.tokenId)
//    val environment = config.getString("environment", "production") match {
//      case "production" => Server.Production
//      case _            => Server.Local
//    }
//
//    ReportGrid(auditToken, environment)
  }
}

object NoopTrackingClient extends ReportGridTrackingClient[JValue](JsonBlueEyes) {
  override def track(path: com.reportgrid.api.Path, name: String, properties: JValue = JsonBlueEyes.EmptyObject, rollup: Boolean = false, timestamp: Option[Date] = None, count: Option[Int] = None, headers: Map[String, String] = Map.empty): Unit = {
    //println("Tracked " + path + "; " + name + " - " + properties)
  }
}


object TestAnalyticsServer extends BlueEyesServer with AnalyticsService {
  def mongoFactory(config: ConfigMap) = {
    new blueeyes.persistence.mongo.mock.MockMongo()
  }

  def auditClientFactory(config: ConfigMap) = ReportGrid(Token.Test.tokenId, Server.Local)
}

// vim: set ts=4 sw=4 et:
