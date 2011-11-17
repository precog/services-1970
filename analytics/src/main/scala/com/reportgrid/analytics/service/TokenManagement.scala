package com.reportgrid
package analytics
package service

import blueeyes._
import blueeyes.concurrent.Future
import blueeyes.core.service._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service.RestPathPattern._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.util.Clock

import org.joda.time.DateTime
import net.lag.logging.Logger

import scalaz.Scalaz._
import scalaz.{Validation, Success, Failure}

object TokenService extends HttpRequestHandlerCombinators {
  def apply(tokenManager: TokenStorage, clock: Clock, logger: Logger): HttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] = {
    path(/?) {
      get { 
        (request: HttpRequest[Future[JValue]]) => (token: Token) => {
          tokenManager.listDescendants(token) map { 
            _.map(_.tokenId).serialize.ok
          }
        }
      } ~
      post { 
        (request: HttpRequest[Future[JValue]]) => (parent: Token) => {
          request.content map { 
            _.flatMap { content => 
              val path        = (content \ "path").deserialize[Option[String]].getOrElse("/")
              val permissions = (content \ "permissions").deserialize(Permissions.permissionsExtractor(parent.permissions))
              val expires     = (content \ "expires").deserialize[Option[DateTime]].getOrElse(parent.expires)
              val limits      = (content \ "limits").deserialize(Limits.limitsExtractor(parent.limits))

              if (expires < clock.now()) {
                Future.sync(HttpResponse[JValue](BadRequest, content = Some("Your are attempting to create an expired token. Such a token will not be usable.")))
              } else tokenManager.issueNew(parent, path, permissions, expires, limits) map {
                case Success(newToken) => HttpResponse[JValue](content = Some(newToken.tokenId.serialize))
                case Failure(message) => throw new HttpException(BadRequest, message)
              }
            }
          } getOrElse {
            Future.sync(HttpResponse[JValue](BadRequest, content = Some("New token must be contained in POST content")))
          }
        }
      }
    } ~
    path("/") {
      path('descendantTokenId) {
        get { 
          (request: HttpRequest[Future[JValue]]) => (token: Token) => {
            if (token.tokenId == request.parameters('descendantTokenId)) {
              token.parentTokenId.map { parTokenId =>
                tokenManager.lookup(parTokenId).map { parent => 
                  val sanitized = parent.map(token.relativeTo).map(_.copy(parentTokenId = None, accountTokenId = ""))
                  HttpResponse[JValue](content = sanitized.map(_.serialize))
                }
              } getOrElse {
                Future.sync(HttpResponse[JValue](Forbidden))
              }
            } else {
              tokenManager.getDescendant(token, request.parameters('descendantTokenId)).map { 
                _.map(_.relativeTo(token).copy(accountTokenId = "").serialize)
              } map { descendantToken =>
                HttpResponse[JValue](content = descendantToken)
              }
            }
          }
        } ~
        delete { 
          (request: HttpRequest[Future[JValue]]) => (token: Token) => {
            tokenManager.deleteDescendant(token, request.parameters('descendantTokenId)).map { _ =>
              HttpResponse[JValue](content = None)
            } ifCanceled { error => 
              error.foreach(logger.warning("An error occurred deleting the token: " + request.parameters('descendantTokenId), _))
            } orElse {
              HttpResponse[JValue](HttpStatus(BadRequest, "No token with id " + request.parameters('descendantTokenId) + " could be found."), content = None)
            } 
          }
        } ~ 
        path("/children") {
          get { 
            (request: HttpRequest[Future[JValue]]) => (token: Token) => {
              tokenManager.lookup(request.parameters('descendantTokenId))
              .flatMap(_.map(tokenManager.listChildren).getOrElse(Future.sync(List.empty[Token])))
              .map(children => HttpResponse[JValue](content = Some(children.map(_.tokenId).serialize)))
            }
          }
        }
      }
    }
  }
}

class TokenRequiredService[A, B](tokenManager: TokenManager, val delegate: HttpService[A, Token => Future[B]])(implicit err: (HttpFailure, String) => B) 
extends DelegatingService[A, Future[B], A, Token => Future[B]] {
  val service = (request: HttpRequest[A]) => {
    request.parameters.get('tokenId) match {
      case None => DispatchError(BadRequest, "A tokenId query parameter is required to access this URL").fail

      case Some(tokenId) =>
        delegate.service(request) map { (f: Token => Future[B]) =>
          tokenManager.lookup(tokenId) flatMap { 
            case None =>                           Future.sync(err(BadRequest,   "The specified token does not exist"))
            case Some(token) if (token.expired) => Future.sync(err(Unauthorized, "The specified token has expired"))

            case Some(token) => f(token)
          }
        }
    }
  }

  val metadata = Some(AboutMetadata(ParameterMetadata('tokenId, None), DescriptionMetadata("A ReportGrid account token is required for the use of this service.")))
}

// vim: set ts=4 sw=4 et:
