package com.reportgrid
package vistrack

import net.lag.configgy.Config

import blueeyes.BlueEyesServer
import blueeyes.BlueEyesServiceBuilder
import blueeyes.concurrent.Future
import blueeyes.concurrent.Future._

import blueeyes.core.http.HttpHeaders
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.combinators.HttpRequestCombinators
import blueeyes.core.http.MimeTypes._
import blueeyes.core.http.{HttpStatusCodes, HttpStatus, HttpRequest, HttpResponse}
import blueeyes.core.data.{BijectionsChunkJson, ByteChunk}
import blueeyes.core.data.Bijection._

import blueeyes.json.JsonAST._

trait Vistrack extends BlueEyesServiceBuilder with HttpRequestCombinators {
  def mongoFactory(configMap: ConfigMap): Mongo

  val Service = service("vistrack", "1.0") { 
    startup {
      import context._
      val mongoConfig = config.configMap("mongo")
      val mongo = mongoFactory(mongoConfig)
      val database = mongo.database(mongoConfig.getString("database", "analytics-v" + serviceVersion))

      val tokensCollection = mongoConfig.getString("tokensCollection", "tokens")

      Future sync TokenManager(database, tokensCollection)
    } ->
    request { tokenManager =>
      def buildTokenHashes(tok: Token, hashes: List[String]): List[String] = {
        if (tok.tokenId == tok.accountTokenId) 
        
      }

      jsonp {
        path("/auditPath") {
          get { req: HttpRequest[ByteChunk] =>
            tokenManager.lookup(req.parameters('tokenId)).map { token =>
              HttpResponse(content = JArray())
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
