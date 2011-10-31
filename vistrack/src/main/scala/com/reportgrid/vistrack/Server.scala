package com.reportgrid
package vistrack

import com.reportgrid.common.Sha1HashFunction
import com.reportgrid.analytics.TokenManager

import net.lag.configgy._

import blueeyes.BlueEyesServer
import blueeyes.BlueEyesServiceBuilder
import blueeyes.concurrent.Future
import blueeyes.concurrent.Future._

import blueeyes.core.data._
import blueeyes.core.data.Bijection._
import blueeyes.core.data.BijectionsChunkJson._
import blueeyes.core.data.BijectionsChunkFutureJson._
import blueeyes.core.data.BijectionsChunkString._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.{HttpStatusCodes, HttpStatus, HttpRequest, HttpResponse}
import blueeyes.core.http.MimeTypes._
import blueeyes.core.http.combinators.HttpRequestCombinators
import blueeyes.json.xschema.SerializationImplicits._
import blueeyes.json.xschema.DefaultSerialization._

import blueeyes.persistence.mongo._

import blueeyes.json.JsonAST._

import org.apache.commons.codec.binary.Base64

trait Vistrack extends BlueEyesServiceBuilder with HttpRequestCombinators {
  def mongoFactory(configMap: ConfigMap): Mongo

  val Service = service("vistrack", "1.0") { context =>
    startup { 
      import context._
      val mongoConfig = config.configMap("mongo")
      val mongo = mongoFactory(mongoConfig)
      val database = mongo.database(mongoConfig.getString("database", "analytics-v" + serviceVersion))

      val tokensCollection = mongoConfig.getString("tokensCollection", "tokens")

      TokenManager(database, tokensCollection)
    } ->
    request { tokenManager =>
      def buildTokenHashes(tokenId: String, hashes: List[String]): Future[List[String]] = {
        tokenManager.lookup(tokenId).flatMap {
          case Some(token) =>
            val hash = Base64.encodeBase64String(Sha1HashFunction(token.tokenId.getBytes)).take(7)
            token.parentTokenId match {
              case Some(id) if token.tokenId != token.accountTokenId => buildTokenHashes(id, hash :: hashes)
              case _ => Future.sync[List[String]](hash :: hashes)
            }

          case None => Future.sync[List[String]](hashes)
        }
      }

      jsonp {
        path("/auditPath") {
          get { req: HttpRequest[Future[JValue]] =>
            req.parameters.get('tokenId) map { tokenId => 
              buildTokenHashes(tokenId, Nil).map(l => HttpResponse[JValue](content = Some(l.serialize)))
            } getOrElse {
              Future.sync(HttpResponse[JValue](HttpStatus(BadRequest, "tokenId request parameter must be specified")))
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

object Vistrack extends BlueEyesServer with Vistrack {
  def mongoFactory(configMap: ConfigMap): Mongo = {
    new blueeyes.persistence.mongo.RealMongo(configMap)
  }
}
