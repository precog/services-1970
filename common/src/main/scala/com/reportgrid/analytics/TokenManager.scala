package com.reportgrid.analytics

import blueeyes._
import blueeyes.BlueEyesServiceBuilder
import blueeyes.concurrent._
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.core.http.MimeTypes.{application, json}
import blueeyes.persistence.mongo._
import blueeyes.persistence.cache._
import blueeyes.json.JsonAST._
import blueeyes.json.JPath
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._

import net.lag.configgy.ConfigMap

import org.joda.time.DateTime

import java.util.concurrent.TimeUnit._

import scala.util.matching.Regex
import scala.math._
import scalaz.Scalaz._
import scalaz.Validation

object TokenManager {
  def apply(database: Database, tokensCollection: MongoCollection) = {
    val RootTokenJ: JObject      = Token.Root.serialize.asInstanceOf[JObject]
    val TestTokenJ: JObject      = Token.Test.serialize.asInstanceOf[JObject]

    val rootTokenFuture  = database(upsert(tokensCollection).set(RootTokenJ))
    val testTokenFuture  = database(upsert(tokensCollection).set(TestTokenJ))

    (rootTokenFuture zip testTokenFuture) map {
      tokens => new TokenManager(database, tokensCollection)
    }
  }
}

trait TokenStorage {
  def lookup(tokenId: String): Future[Option[Token]]
}


class TokenManager private (database: Database, tokensCollection: MongoCollection) extends TokenStorage {
  //TODO: Add expiry settings.
  val tokenCache = Cache.concurrent[String, Token](CacheSettings(ExpirationPolicy(None, None, MILLISECONDS)))
  tokenCache.put(Token.Root.tokenId, Token.Root)
  tokenCache.put(Token.Test.tokenId, Token.Test)

  private def find(tokenId: String) = database {
    selectOne().from(tokensCollection).where(("deleted" !== true) & ("tokenId" === tokenId))
  }

  /** Look up the specified token.
   */
  def lookup(tokenId: String): Future[Option[Token]] = {
    tokenCache.get(tokenId).map[Future[Option[Token]]](v => Future.sync(Some(v))) getOrElse {
      find(tokenId) map {
        _.map(_.deserialize[Token] ->- (tokenCache.put(tokenId, _)))
      }
    }
  }

  def listChildren(parent: Token): Future[List[Token]] = {
    database {
      selectAll.from(tokensCollection).where {
        ("parentTokenId" === parent.tokenId) &&
        ("tokenId"      !== parent.tokenId) &&
        ("deleted" !== true)
      }
    } map { result =>
      result.toList.map(_.deserialize[Token])
    }
  }

  /** List all descendants of the specified token.
   */
  def listDescendants(parent: Token): Future[List[Token]] = {
    listChildren(parent).flatMap { children =>
      Future[List[Token]](children.map { child =>
        for {
          descendantsOfChild <- listDescendants(child)
        } yield child :: descendantsOfChild
      }: _*).map { (nested: List[List[Token]]) =>
        nested.flatten
      }
    }
  }

  /** Issue a new token from the specified token.
   */
  def issueNew(parent: Token, path: Path, permissions: Permissions, expires: DateTime, limits: Limits): Future[Validation[String, Token]] = {
    if (parent.canShare) {
      val newToken = if (parent == Token.Root) {
        // This is the root token being used to create a new account:
        Token.newAccount(path, limits, permissions, expires)
      } else {
        // This is a customer token being used to create a child token:
        parent.issue(path, permissions, expires, limits)
      }

      val tokenJ = newToken.serialize.asInstanceOf[JObject]
      database(insert(tokenJ).into(tokensCollection)) map (_ => newToken.success)
    } else {
      Future.sync(("Token " + parent + " does not allow creation of child tokens.").fail)
    }
  }

  /** Get details about a specified child token.
   */
  def getDescendant(parent: Token, descendantTokenId: String): Future[Option[Token]] = {
    listDescendants(parent).map(_.find(_.tokenId == descendantTokenId))
  }

  /** Delete a specified child token.
   */
  def deleteDescendant(parent: Token, descendantTokenId: String): Future[Option[Token]] = {
    for  {
      Some(descendant) <- getDescendant(parent, descendantTokenId)
      _ <- database(update(tokensCollection).set(JPath("deleted").set(true)).where("tokenId" === descendantTokenId))
    } yield {
      tokenCache.remove(descendantTokenId)
    }
  }
}
