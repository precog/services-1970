package com.reportgrid.analytics
package service

import blueeyes.concurrent.Future
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.DefaultSerialization._

trait AnalyticsServiceCombinators extends HttpRequestHandlerCombinators {
  implicit val jsonErrorTransform = (failure: HttpFailure, s: String) => HttpResponse(failure, content = Some(s.serialize))

  def token[A, B](tokenManager: TokenManager)(service: HttpService[A, Token => Future[B]])(implicit err: (HttpFailure, String) => B) = {
    new TokenRequiredService[A, B](tokenManager, service)
  }

  def vfsPath[A, B](next: HttpService[A, (Token, Path) => Future[B]]) = {
    path("""/vfs/(?:(?<prefixPath>(?:[^\n.](?:[^\n/]|/[^\n\.])+)/?)?)""") { 
      new DelegatingService[A, Token => Future[B], A, (Token, Path) => Future[B]] {
        val delegate = next
        val service = (request: HttpRequest[A]) => {
          val path: Option[String] = request.parameters.get('prefixPath).filter(_ != null) 
          next.service(request) map { f => (token: Token) => f(token, path.map(token.path / _).getOrElse(token.path)) }
        }

        val metadata = None
      }
    }
  }

  def variable[A, B](delegate: HttpService[A, (Token, Path, Variable) => Future[B]]): HttpService[A, (Token, Path) => Future[B]] = {
    path("""(?<variable>\.[^\n/]+)""") {
      parameter('variable) {
        delegate.map(f => (s: String) => f(_: Token, _: Path, Variable(JPath(s))))
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
