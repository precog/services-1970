package com.reportgrid.analytics
import  external._

import blueeyes.BlueEyesServer
import blueeyes.concurrent.Future
import blueeyes.json.JsonAST._
import blueeyes.persistence.mongo.Mongo
import blueeyes.persistence.mongo.MongoCollection
import blueeyes.persistence.mongo.Database
import blueeyes.util.Clock

import com.reportgrid.api.Server
import com.reportgrid.api.blueeyes.ReportGrid

import java.util.Date
import net.lag.configgy.ConfigMap

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

  def jessup(configMap: ConfigMap): Jessup = {
    new JessupServiceProxy(
      configMap.getString("host", "api.reportgrid.com"),
      configMap.getInt("port"),
      configMap.getString("path", "/services/jessup/v1"))
  }

  def tokenManager(database: Database, tokensCollection: MongoCollection, deletedTokensCollection: MongoCollection): TokenManager = 
    new TokenManager(database, tokensCollection, deletedTokensCollection)

  val clock = Clock.System
}

// vim: set ts=4 sw=4 et:
