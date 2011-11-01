package com.reportgrid
package vistrack

import com.reportgrid.common.Sha1HashFunction
import com.reportgrid.analytics.TokenStorage
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
import blueeyes.core.service._
import blueeyes.json.xschema.SerializationImplicits._
import blueeyes.json.xschema.DefaultSerialization._

import blueeyes.persistence.mongo._

import blueeyes.json.JsonAST._

import org.apache.commons.codec.binary.Hex

trait Vistrack extends BlueEyesServiceBuilder with HttpRequestCombinators {
  def tokenStorage(configMap: ConfigMap, serviceVersion: ServiceVersion): Future[TokenStorage]

  def buildTokenHashes(tokenStorage: TokenStorage, tokenId: String, hashes: List[String]): Future[List[String]] = {
    tokenStorage.lookup(tokenId).flatMap {
      case Some(token) =>
        val hash = Hex.encodeHexString(Sha1HashFunction(token.tokenId.getBytes)).take(7)
        token.parentTokenId match {
          case Some(id) if token.tokenId != token.accountTokenId => buildTokenHashes(tokenStorage, id, hash :: hashes)
          case _ => Future.sync[List[String]](hash :: hashes)
        }

      case None => Future.sync[List[String]](hashes)
    }
  }

  val Service = service("vistrack", "1.0") { context =>
    startup { 
      import context._
      tokenStorage(config, serviceVersion)
    } ->
    request { tokenStorage =>
      jsonp {
        path("/auditPath") {
          get { req: HttpRequest[Future[JValue]] =>
            req.parameters.get('tokenId) map { tokenId => 
              buildTokenHashes(tokenStorage, tokenId, Nil).map(l => HttpResponse[JValue](content = Some(l.mkString("/").serialize)))
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
  def tokenStorage(configMap: ConfigMap, serviceVersion: ServiceVersion): Future[TokenStorage] = {
    val mongoConfig = config.configMap("mongo")
    val mongo = new blueeyes.persistence.mongo.RealMongo(configMap)

    TokenManager(
      mongo.database(mongoConfig.getString("database", "analytics-v" + serviceVersion)),
      mongoConfig.getString("tokensCollection", "tokens")
    ).map(a => a: TokenStorage)
  }
}
