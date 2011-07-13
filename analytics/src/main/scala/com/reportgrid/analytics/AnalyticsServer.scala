package com.reportgrid.analytics

import blueeyes.BlueEyesServer
import blueeyes.persistence.mongo.Mongo
import com.reportgrid.api.Server
import com.reportgrid.api.blueeyes.ReportGrid
import net.lag.configgy.ConfigMap

object AnalyticsServer extends BlueEyesServer with AnalyticsService {
  def mongoFactory(configMap: ConfigMap): Mongo = {
    new blueeyes.persistence.mongo.RealMongo(configMap)
  }

  def auditClientFactory(config: ConfigMap) = {
    val auditToken = config.getString("token", Token.Test.tokenId)
    val environment = config.getString("environment", "production") match {
      case "production" => Server.Production
      case _            => Server.Local
    }

    ReportGrid(auditToken, environment)
  }
}

object TestAnalyticsServer extends BlueEyesServer with AnalyticsService {
  def mongoFactory(config: ConfigMap) = {
    new blueeyes.persistence.mongo.mock.MockMongo()
  }

  def auditClientFactory(config: ConfigMap) = ReportGrid(Token.Test.tokenId, Server.Local)
}

// vim: set ts=4 sw=4 et:
