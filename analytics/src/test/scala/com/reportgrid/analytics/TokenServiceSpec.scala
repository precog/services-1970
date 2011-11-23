package com.reportgrid.analytics
package service

import blueeyes._
import blueeyes.core.http._
import blueeyes.concurrent.Future
import blueeyes.concurrent.test._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.JPathImplicits._
import blueeyes.util.Clock

import org.joda.time._
import net.lag.configgy.ConfigMap
import net.lag.logging.Logger

import org.specs2.mutable.Specification
import org.specs2.specification.{Outside, Scope}
import org.scalacheck.Gen._

import scalaz.{Success, Validation}
import scalaz.Scalaz._

class TokenServiceSpec extends Specification with FutureMatchers with TestTokenStorage with TestTokens with scalaz.Trees {
  import scalaz.Tree
  
  val clock = Clock.System
  val tokenService = TokenService(tokenManager, clock, Logger.get("test"))

  object sampleData extends Outside[Tree[Token]] with Scope {
    def buildChildTokens(tok: Token, depth: Int): Tree[Token] = {
      if (depth == 0) leaf(tok)
      else node(
        tok, 
        (0 to choose(1, 3).sample.get).toStream.map { _ => 
          buildChildTokens(tok.issue(relativePath = identifier.sample.get), choose(0, depth - 1).sample.get)
        }.force
      )
    }

    val outside = buildChildTokens(TestToken, choose(2, 4).sample.get) map {
      t => tokenCache.put(t.tokenId, t); t
    }
  }

  "Tokens Service" should {
    "return the root token" in sampleData { sampleTokens =>
      val tok = sampleTokens.rootLabel

      tokenService.service(HttpRequest(HttpMethods.GET, URI("/" + tok.tokenId))) must beLike {
        case Success(f) => f(tok) must whenDelivered {
          beLike {
            case HttpResponse(HttpStatus(HttpStatusCodes.OK, _), _, Some(content), _) =>
              content.deserialize[Token] must beLike {
                case Token(tokenId, None, "", path, permissions, _, limits) =>
                  (tokenId must_== tok.tokenId) and
                  (path must_== tok.path) and
                  (permissions must_== tok.permissions) and
                  (limits must_== tok.limits) 
              }
          }
        }
      }
    }

    "return token children" in sampleData { sampleTokens =>
      val node = sampleTokens.subForest.head

      tokenService.service(HttpRequest(HttpMethods.GET, URI("/children"))) must beLike {
        case Success(f) => f(node.rootLabel) must whenDelivered {
          beLike {
            case HttpResponse(HttpStatus(HttpStatusCodes.OK, _), _, Some(content), _) =>
              content.deserialize[List[String]] must haveTheSameElementsAs(node.subForest.map(_.rootLabel.tokenId))
          }
        }
      }
    }
  }
}
