package com.reportgrid.analytics

import blueeyes.BlueEyesServiceBuilder
import blueeyes.concurrent.{Future, FutureDeliveryStrategySequential}
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.core.http.MimeTypes.{application, json}
import blueeyes.persistence.mongo._
import blueeyes.persistence.cache.{Stage, ExpirationPolicy, CacheSettings}
import blueeyes.json.JsonAST._
import blueeyes.json.JPath
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._

import net.lag.configgy.ConfigMap

import org.joda.time.{DateTime, DateTimeZone}

import java.util.concurrent.TimeUnit

import scala.util.matching.Regex
import scala.math._

import com.reportgrid.analytics.AggregatorImplicits._
import com.reportgrid.analytics.persistence.MongoSupport._

object TokenManager {
  def apply(database: MongoDatabase, tokensCollection: MongoCollection) = {
    val RootTokenJ: JObject = Token.Root.serialize.asInstanceOf[JObject]
    val TestTokenJ: JObject = Token.Test.serialize.asInstanceOf[JObject]

    val rootTokenFuture = database[JNothing.type](upsert(tokensCollection).set(RootTokenJ)).toUnit
    val testTokenFuture = database[JNothing.type](upsert(tokensCollection).set(TestTokenJ)).toUnit

    (rootTokenFuture zip testTokenFuture) map {
      tokens => new TokenManager(database, tokensCollection)
    }
  }
}


class TokenManager private (database: MongoDatabase, tokensCollection: MongoCollection) extends FutureDeliveryStrategySequential {

  /** Look up the specified token.
   */
  def lookup(tokenId: String): Future[Option[Token]] = {
    database {
      selectOne().from(tokensCollection).where("tokenId" === tokenId)
    }.map(_.map(_.deserialize[Token]))
  }

  def listChildren(parent: Token): Future[List[Token]] = {
    database {
      selectAll.from(tokensCollection).where {
        ("parentTokenId" === parent.tokenId) &&
        ("tokenId"      !== parent.tokenId)
      }
    }.map { result =>
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
      val newToken = ((parent == Token.Root) match {
        // This is a customer token being used to create a child token:
        case false => parent.issue(path, permissions, expires, limits)

        // This is the root token being used to create a new account:
        case true => Token.newAccount(path, limits, permissions, expires)
      })

      val tokenJ = newToken.serialize.asInstanceOf[JObject]

      database[JNothing.type] {
        insert(tokenJ).into(tokensCollection)
      }.map(_ => newToken)
    }
    else Future.dead(new Exception("Token " + parent + " does not have permission to share"))
  }

  /** Get details about a specified child token.
   */
  def getDescendant(parent: Token, descendantTokenId: String): Future[Option[Token]] = {
    listDescendants(parent).map { descendants =>
      descendants.find(_.tokenId == descendantTokenId)
    }
  }

  /** Delete a specified child token.
   */
  def deleteDescendant(parent: Token, descendantTokenId: String): Future[Option[Token]] = {
    getDescendant(parent, descendantTokenId).deliverTo { descendant =>
      descendant.foreach { descendantToken =>
        database[JNothing.type] {
          remove.from(tokensCollection).where {
            "tokenId" === descendantTokenId
          }
        }
      }
    }
  }
}
