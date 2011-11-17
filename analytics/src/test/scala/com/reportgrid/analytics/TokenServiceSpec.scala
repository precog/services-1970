package com.reportgrid.analytics
package service

import blueeyes._
import blueeyes.core.http._
import blueeyes.concurrent.Future
import blueeyes.concurrent.test._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.JPathImplicits._

import org.joda.time._
import net.lag.configgy.ConfigMap
import net.lag.logging.Logger

import org.specs2.mutable.Specification
import org.specs2.specification.{Outside, Scope}
import org.scalacheck.Gen._

import scalaz.{Success, Validation}
import scalaz.Scalaz._

class TokenServiceSpec extends TestAnalyticsService with FutureMatchers with scalaz.Trees {
  import scalaz.Tree
  
  val tokenCache = new scala.collection.mutable.HashMap[String, Token]
  val tokenManager = new TokenStorage {
    tokenCache.put(Token.Root.tokenId, Token.Root)

    def lookup(tokenId: String): Future[Option[Token]] = Future.sync(tokenCache.get(tokenId))
    def listChildren(parent: Token): Future[List[Token]] = Future.sync(
      tokenCache flatMap { case (_, v) => v.parentTokenId.exists(_ == parent.tokenId).option(v) } toList 
    )

    def issueNew(parent: Token, path: Path, permissions: Permissions, expires: DateTime, limits: Limits): Future[Validation[String, Token]] = {
      val newToken = parent.issue(path, permissions, expires, limits)
      tokenCache.put(newToken.tokenId, newToken)
      Future.sync(newToken.success[String])
    }

    def deleteDescendant(parent: Token, descendantTokenId: String): Future[Option[Token]] = {
      Future.sync(tokenCache.remove(descendantTokenId))
    }
  }

  val tokenService = TokenService(tokenManager, clock, Logger.get("test"))

  object sampleData extends Outside[Tree[Token]] with Scope {
    def buildChildTokens(tok: Token, depth: Int): Tree[Token] = {
      if (depth == 0) leaf(tok)
      else node(
        tok, 
        (0 to choose(0, 3).sample.get).toStream.map { _ => 
          buildChildTokens(tok.issue(relativePath = identifier.sample.get), choose(0, depth - 1).sample.get)
        }
      )
    }

    val outside = buildChildTokens(TestToken, choose(3, 5).sample.get) ->- {
      _.map(t => tokenCache.put(t.tokenId, t))
    }
  }

  "Tokens Service" should {
    "return the root token" in sampleData { sampleTokens =>
      val tok = sampleTokens.rootLabel

      tokenService.service(HttpRequest(HttpMethods.GET, URI("/" + tok.tokenId))) must beLike {
        case Success(f) => f(tok) must whenDelivered {
          beLike {
            case HttpResponse(HttpStatus(status, _), _, Some(content), _) =>
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
  }
}
