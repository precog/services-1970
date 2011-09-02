package com.reportgrid.analytics

import blueeyes.BlueEyesServer
import blueeyes.core.http._
import blueeyes.json.JsonAST._
import blueeyes.persistence.mongo.Mongo
import com.reportgrid.api.Server
import com.reportgrid.api.blueeyes.ReportGrid
import net.lag.configgy.ConfigMap

object AnalyticsServer extends BlueEyesServer with AnalyticsService {
  override def mongoFactory(configMap: ConfigMap): Mongo = new blueeyes.persistence.mongo.RealMongo(configMap)

  override def auditClientFactory(config: ConfigMap) = {
    val auditToken = config.getString("token", Token.Test.tokenId)
    val environment = config.getString("environment", "production") match {
      case "production" => Server.Production
      case _            => Server.Local
    }

    ReportGrid(auditToken, environment)
  }

  override def v1Rewrite(req: HttpRequest[JValue], conf: ForwardingConfig): Option[HttpRequest[JValue]] = {
    import HttpHeaders._
    val content = req.content.map {
      case JObject(fields) => JObject(fields.map {
        case JField("timestamp", value) => JField("#timestamp", value)
      })

      case v => v // should be an error, but for now just forwarding it on
    }

    content.map { forwardContent =>
      val prefixPath = req.parameters.get('prefixPath).getOrElse("")
      req.copy(
        uri = req.uri.copy(
          host = Some(conf.host), 
          port = conf.port, 
          path = Some(conf.path + "/vfs/" + prefixPath)
        ),
        parameters = req.parameters - 'prefixPath,
        content = Some(forwardContent)
      ) 
    }
  }
}

object TestAnalyticsServer extends BlueEyesServer with AnalyticsService {
  override def mongoFactory(config: ConfigMap) = new blueeyes.persistence.mongo.mock.MockMongo()
  override def auditClientFactory(config: ConfigMap) = ReportGrid(Token.Test.tokenId, Server.Local)
  override def v1Rewrite(req: HttpRequest[JValue], conf: ForwardingConfig) = None
}

// vim: set ts=4 sw=4 et:
