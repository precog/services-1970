package com.reportgrid
package analytics
package service

import blueeyes.concurrent.Future
import blueeyes.core.service._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._

import scalaz.Scalaz._

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
