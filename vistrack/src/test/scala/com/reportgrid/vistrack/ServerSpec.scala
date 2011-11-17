package com.reportgrid.vistrack

import com.reportgrid.analytics
import com.reportgrid.analytics._

import blueeyes.concurrent.Future
import blueeyes.concurrent.test._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.core.service.test.BlueEyesServiceSpecification
import MimeTypes._
import blueeyes.json.JsonAST._
import net.lag.configgy.ConfigMap

import org.specs2.mutable._

import BijectionsChunkJson._
import BijectionsChunkString._
import BijectionsChunkFutureJson._

class VistrackServiceSpec extends BlueEyesServiceSpecification with Vistrack with FutureMatchers {
  val RootToken = Token.Test
  val ChildToken = RootToken.issue()
  val GrandchildToken = ChildToken.issue()

  lazy val jsonTestService = service.contentType[JValue](application/json)

  def tokenStorage(configMap: ConfigMap, serviceVersion: ServiceVersion) = Future sync {
    new TokenStorage {
      def lookup(tokenId: String): Future[Option[Token]] = {
        tokenId match {
          case RootToken.tokenId => Future.sync(Some(RootToken))
          case ChildToken.tokenId => Future.sync(Some(ChildToken))
          case GrandchildToken.tokenId => Future.sync(Some(GrandchildToken))
          case _ => Future.sync(None)
        }
      }
    }
  }

  "the vistrack service" should {
    "return a path containing all tokens from the root to the leaf" in {
      jsonTestService.query("tokenId", GrandchildToken.tokenId).get[JValue]("/auditPath") must whenDelivered {
        beLike {
          case HttpResponse(HttpStatus(OK, _), _, Some(JString(path)), _) => 
            val p = analytics.Path(path)
            println(p.elements)
            p.length must_== 3
        }
      }
    }
  }
}
// vim: set ts=4 sw=4 et:
