package com.reportgrid.analytics

import blueeyes.BlueEyesServer
import blueeyes.core.http._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.JodaSerializationImplicits._
import blueeyes.persistence.mongo.Mongo
import com.reportgrid.api.Server
import com.reportgrid.api.blueeyes.ReportGrid
import net.lag.configgy.ConfigMap
import net.lag.logging.Logger
import org.joda.time.Instant

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

  override def v1Rewrite(logger: Logger, req: HttpRequest[JValue], conf: ForwardingConfig): Option[HttpRequest[JValue]] = {
    import HttpHeaders._
    req.content.map { content =>
      val count = (content \ "count") match {
        case JInt(value) => Some(value)
        case _ => None
      }

      val timestamp = (content \ "timestamp") match {
        case JNothing | JNull => None
        case jvalue => Some(jvalue.deserialize[Instant])
      }

      val events: JValue = (content \ "events") match {
        case JObject(fields) => 
          JObject(
            fields.flatMap {
              case JField(eventName, JObject(metadata)) => 
                Some(JField(eventName, JObject(metadata ++ timestamp.map(instant => JField("#timestamp", instant.serialize)))))

              case _ => None
            }
          )

        case err => JNothing
      }

      val prefixPath = req.parameters.get('prefixPath).getOrElse("")
      val forwarding = req.copy(
        uri = req.uri.copy(host = Some(conf.host), port = conf.port, path = Some(conf.path + "/vfs/" + prefixPath)),
        parameters = (req.parameters - 'prefixPath) ++ (count.map(v => 'count -> v.toString).toMap),
        content = Some(events)
      ) 

      logger.debug("Forwarding request from v0 " + req + " as " + forwarding)
      
      forwarding
    }
  }
}

object TestAnalyticsServer extends BlueEyesServer with AnalyticsService {
  override def mongoFactory(config: ConfigMap) = new blueeyes.persistence.mongo.mock.MockMongo()
  override def auditClientFactory(config: ConfigMap) = ReportGrid(Token.Test.tokenId, Server.Local)
  override def v1Rewrite(logger: Logger, req: HttpRequest[JValue], conf: ForwardingConfig) = None
}

// vim: set ts=4 sw=4 et:
