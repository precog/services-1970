package com.reportgrid.analytics
import  service._
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

  def storageReporting(config: ConfigMap) = {
    val token = config.getString("token") getOrElse {
      throw new IllegalStateException("storageReporting.tokenId must be specified in application config file. Service cannot start.")
    }

    val environment = config.getString("environment", "dev") match {
      case "production" => Server.Production
      case "dev"        => Server.Dev
      case _            => Server.Local
    }

    //new ReportGridStorageReporting(token, ReportGrid(token, environment))
    new NullStorageReporting(token)
  }

  def auditClient(config: ConfigMap) = {
    NoopTrackingClient
    //val auditToken = config.configMap("audit.token")
    //val environment = config.getString("environment", "production") match {
    //  case "production" => Server.Production
    //  case _            => Server.Local
    //}

    //ReportGrid(auditToken, environment)
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
