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

import com.reportgrid.analytics.AggregatorImplicits._
import com.reportgrid.analytics.persistence.MongoSupport._

object TokenManager {
  def apply(database: Database, tokensCollection: MongoCollection) = {
    val RootTokenJ: JObject      = Token.Root.serialize.asInstanceOf[JObject]
    val TestTokenJ: JObject      = Token.Test.serialize.asInstanceOf[JObject]
    val BenchmarkTokenJ: JObject = Token.Benchmark.serialize.asInstanceOf[JObject]

    val rootTokenFuture  = database(upsert(tokensCollection).set(RootTokenJ))
    val testTokenFuture  = database(upsert(tokensCollection).set(TestTokenJ))
    val benchTokenFuture = database(upsert(tokensCollection).set(BenchmarkTokenJ))

    (rootTokenFuture zip testTokenFuture zip benchTokenFuture) map {
      tokens => new TokenManager(database, tokensCollection)
    }
  }
}


class TokenManager private (database: Database, tokensCollection: MongoCollection) {
  //TODO: Add expiry settings.
  val tokenCache = Cache.concurrent[String, Token](CacheSettings(ExpirationPolicy(None, None, MILLISECONDS)))
  tokenCache.put(Token.Root.tokenId, Token.Root)
  tokenCache.put(Token.Test.tokenId, Token.Test)
  tokenCache.put(Token.Benchmark.tokenId, Token.Benchmark)

  /** Look up the specified token.
   */
  def lookup(tokenId: String): Future[Option[Token]] = {
    tokenCache.get(tokenId).map[Future[Option[Token]]](v => Future.sync(Some(v))) getOrElse {
      database {
        selectOne().from(tokensCollection).where("tokenId" === tokenId)
      } map {
        _.map(_.deserialize[Token] ->- (tokenCache.put(tokenId, _)))
      }
    }
  }

  def listChildren(parent: Token): Future[List[Token]] = {
    database {
      selectAll.from(tokensCollection).where {
        ("parentTokenId" === parent.tokenId) &&
        ("tokenId"      !== parent.tokenId)
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
  def issueNew(parent: Token, path: Path, permissions: Permissions, expires: DateTime, limits: Limits): Future[Token] = {
    if (parent.canShare) {
      val newToken = if (parent == Token.Root) {
        // This is the root token being used to create a new account:
        Token.newAccount(path, limits, permissions, expires)
      } else {
        // This is a customer token being used to create a child token:
        parent.issue(path, permissions, expires, limits)
      }

      val tokenJ = newToken.serialize.asInstanceOf[JObject]
      database(insert(tokenJ).into(tokensCollection)) map (_ => newToken)
    } else {
      Future.dead(new Exception("Token " + parent + " does not have permission to share"))
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
    getDescendant(parent, descendantTokenId).deliverTo { 
      _.foreach(_ => database(remove.from(tokensCollection).where("tokenId" === descendantTokenId)))
    }
  }
}
