package com.reportgrid.billing

import java.util.UUID
import java.security.SecureRandom

import scala.collection.JavaConverters._

import scalaz._
import scalaz.Scalaz._
import scalaz.Validation._

import org.joda.time.DateTime
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.codec.binary.Hex
import com.braintreegateway._

import com.reportgrid.analytics._
import com.reportgrid.billing.braintree._

import blueeyes.persistence.mongo.Database
import blueeyes.persistence.mongo._
import blueeyes.concurrent.Future
import blueeyes.core.http.HttpRequest
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser
import blueeyes.json.Printer
import blueeyes.core.service._
import blueeyes.core.data._
import blueeyes.core.data.BijectionsChunkJson._
import blueeyes.health.HealthMonitor

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.Extractor._
import Extractor.Error
import blueeyes.json.xschema.DefaultSerialization._

import SerializationHelpers._

abstract class TokenGenerator {
  def newToken(path: String): Future[Validation[String, String]]
  def newChildToken(parent: String, path: String): Future[Validation[String, String]]
  def disableToken(token: String): Future[Unit]
  def deleteToken(token: String): Future[Unit]
}

class MockTokenGenerator extends TokenGenerator {
  override def newToken(path: String) = {
    Future.sync(Success(UUID.randomUUID().toString().toUpperCase()))
  }

  override def newChildToken(parent: String, path: String) = {
    Future.sync(Success(UUID.randomUUID().toString().toUpperCase()))
  }

  override def deleteToken(token: String) = {
    Future.sync(Unit)
  }
  
  override def disableToken(token: String) = {
    Future.sync(Unit)    
  }
}

class RealTokenGenerator(client: HttpClient[ByteChunk], rootToken: String, rootUrl: String) extends TokenGenerator {

  import blueeyes.core.http.MimeTypes._

  val defaultOrder = 2
  val defaultLimit = 10
  val defaultDepth = 3
  val defaultTags = 1

  override def newToken(path: String) = {
    client.
      query("tokenId", rootToken).
      contentType[ByteChunk](application / json).
      post[JValue](rootUrl)(JsonParser.parse(
        """
          {
            "path": , "%s"
            "permissions": {
                "read":    true,
                "write":   true,
                "share":   true,
                "explore": true
            },
            "limits": {
                "order": %d,
                "limit": %d,
                "depth": %d,
                "tags" : %d
            }
          }
        """.format(path, defaultOrder, defaultLimit, defaultDepth, defaultTags)))
  }.map { h =>
    h.content match {
      case Some(JString(s)) => Success(s)
      case _ => Failure("Unable to create new token")
    }
  }
  
  override def newChildToken(parent: String, path: String) = {
    client.
      query("tokenId", parent).
      contentType[ByteChunk](application / json).
      post[JValue](rootUrl)(JsonParser.parse(
        """
          {
            "path": , "%s"
          }
        """.format(path)))
  }.map { h =>
    h.content match {
      case Some(JString(s)) => Success(s)
      case _ => Failure("Unable to create new token")
    }
  }

  override def deleteToken(token: String) = {
    client.
      query("tokenId", rootToken).
      contentType[ByteChunk](application / json).
      delete[JValue](rootUrl + token)
  }.map(_ => Unit)
  
  override def disableToken(token: String) = {
    Future.sync(Success(Unit))
  }
}

object BillingServiceHandlers {

  private def jerror(message: String): JValue = JString(message)
  private def jerror(error: Error): JValue = jerror(error.message)

  private def collapseToHttpResponse(v: Validation[String, JValue]): HttpResponse[JValue] = v match {
    case Success(jv) => HttpResponse[JValue](content = Some(jv))
    case Failure(e) => HttpResponse(HttpStatus(400, e), content = Some(e))
  }

  class CreateAccountHandler(config: BillingConfiguration, monitor: HealthMonitor) extends AccountAccessHandler(config) {

    private val mailer = config.mailer

    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
      (request.content match {
        case None => {
          monitor.count(".accounts.create.failure")
          config.sendSignupFailure(None, "Request content missing.")
          Future.sync[Validation[String, Account]](failure("Missing request content"))
        }
        case Some(js) => js.flatMap[Validation[String, Account]] {
          _.validated[CreateAccount] match {
            case Success(ca) => {
              val createResult = accounts.openAccount(ca)
              createResult.map {
                case s @ Success(a) => {
                  config.sendSignupSuccess(a)
                  s
                }
                case f @ Failure(e) => {
                  monitor.count(".accounts.create.failure")
                  config.sendSignupFailure(Some(ca), "Error processing CreateAccount request: [" + e + "]")
                  f
                }
              }
            }
            case Failure(e) => {
              monitor.count(".accounts.create.failure")
              config.sendSignupFailure(None, "Error parsing json request into CreateAccount: [" + e.message + "]")
              Future.sync(Failure(e.message))
            }
          }
        }
      }).map[HttpResponse[JValue]](x => collapseToHttpResponse(x.map(_.serialize)))
    }
  }

  // Notes
  // * requires id, token and password?
  // * what to do with cancelled accounts?
  // * simply flag as canceled?
  // * email confirmation of cancellation?

  // Input
  // * accountId
  // * password hash

  // Output

  abstract class AccountAccessHandler(config: BillingConfiguration) extends HttpServiceHandler[Future[JValue], Future[HttpResponse[JValue]]] {
    val accounts = config.naccounts
  }
  
  class CloseAccountHandler(config: BillingConfiguration, monitor: HealthMonitor) extends AccountAccessHandler(config) {
    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
      (request.content match {
        case None => Future.sync[Validation[String, Account]](Failure("Empty content"))
        case Some(x) => x.flatMap[Validation[String, Account]] {
          _.validated[AccountAuthentication] match {
            case Success(a) => {
              accounts.closeAccount(a)
            }
            case Failure(e) => {
              monitor.count(".accounts.get.failure")
              Future.sync(Failure(e.message))
            }
          }
        }
      }).map[HttpResponse[JValue]](x => collapseToHttpResponse(x.map(_.serialize)))
    }
  }

  private def fvApply[E, A, B](r: Future[Validation[E, A]], f: A => Future[Validation[E, B]]): Future[Validation[E, B]] = {
    r.flatMap{ _.map(f) match {
      case Success(fva) => fva
      case f @ Failure(s) => Future.sync(Failure(s))
    }}
  }  

  class UpdateBillingHandler(config: BillingConfiguration, monitor: HealthMonitor) extends AccountAccessHandler(config) {
    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
      (request.content match {
        case None => {
          monitor.count(".accounts.get.failure")
          Future.sync[Validation[String, BillingInformation]](failure("Missing request content"))
        }
        case Some(x) => x.flatMap[Validation[String, BillingInformation]] {
          _.validated[UpdateBilling] match {
            case Success(ub) => {
              accounts.updateBillingInformation(ub)
            }
            case Failure(e) => {
              monitor.count(".accounts.get.failure")
              Future.sync(failure(e.message))
            }
          }
        }
      }).map[HttpResponse[JValue]](x => collapseToHttpResponse(x.map(_.serialize)))
    }
  }
  
  class GetBillingHandler(config: BillingConfiguration, monitor: HealthMonitor) extends AccountAccessHandler(config) {
    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
      (request.content match {
        case None => {
          monitor.count(".accounts.get.failure")
          Future.sync[Validation[String, Option[BillingInformation]]](failure("Missing request content"))
        }
        case Some(x) => x.flatMap[Validation[String, Option[BillingInformation]]] {
          _.validated[AccountAuthentication] match {
            case Success(a) => {
              accounts.getBillingInformation(a)
            }
            case Failure(e) => {
              monitor.count(".accounts.get.failure")
              Future.sync(failure(e.message))
            }
          }
        }
      }).map[HttpResponse[JValue]](x => collapseToHttpResponse(x.map(_.serialize)))
    }
  }

  class RemoveBillingHandler(config: BillingConfiguration, monitor: HealthMonitor) extends AccountAccessHandler(config) {
    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
      (request.content match {
        case None => {
          monitor.count(".accounts.get.failure")
          Future.sync[Validation[String, Option[BillingInformation]]](failure("Missing request content"))
        }
        case Some(x) => x.flatMap[Validation[String, Option[BillingInformation]]] {
          _.validated[AccountAuthentication] match {
            case Success(a) => {
              accounts.removeBillingInformation(a)
            }
            case Failure(e) => {
              monitor.count(".accounts.get.failure")
              Future.sync(failure(e.message))
            }
          }
        }
      }).map[HttpResponse[JValue]](x => collapseToHttpResponse(x.map(_.serialize)))
    }
  }

  // Notes
  // * requires id, token and password (respec all or allow partial structure?)
  // * how to handle billing implications?
  // * email confirmation of service changes?
  class UpdateAccountHandler(config: BillingConfiguration, monitor: HealthMonitor) extends AccountAccessHandler(config) {
    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
      (request.content match {
        case None => Future.sync[Validation[String, AccountInformation]](Failure("Missing update request content."))
        case Some(js) => js.flatMap[Validation[String, AccountInformation]] {
          _.validated[UpdateAccount] match {
            case Success(ua) => accounts.updateAccountInformation(ua)
            case Failure(e)  => Future.sync(Failure(e.message))
          }
        }
      }).map[HttpResponse[JValue]](x => collapseToHttpResponse(x.map(_.serialize)))
    }    
  }

  class GetAccountHandler(config: BillingConfiguration, monitor: HealthMonitor) extends AccountAccessHandler(config) {
    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
      (request.content match {
        case None => {
          monitor.count(".accounts.get.failure")
          Future.sync[Validation[String, AccountInformation]](failure("Missing request content"))
        }
        case Some(x) => x.flatMap[Validation[String, AccountInformation]] {
          _.validated[AccountAuthentication] match {
            case Success(a) => {
              try {
                accounts.getAccountInformation(a).orElse{ot => 
                  Failure(ot.map(t => 
                    {t.printStackTrace(System.out); "Caught exception."}
                    ).getOrElse("Shouldn't happen"))
                }
              } catch {
                case ex => ex.printStackTrace(System.out); throw ex
              }
            }
            case Failure(e) => {
              monitor.count(".accounts.get.failure")
              Future.sync(failure(e.message))
            }
          }
        }
      }).map[HttpResponse[JValue]](x => collapseToHttpResponse(x.map(_.serialize)))
    }
  }

//  class CloseAccountHandler(config: BillingConfiguration, monitor: HealthMonitor) extends AccountAccessHandler(config) {
//    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
//      (request.content match {
//        case None => {
//          monitor.count(".accounts.get.failure")
//          Future.sync[Validation[String, Account]](failure("Missing request content"))
//        }
//        case Some(x) => x.flatMap[Validation[String, Account]] {
//          _.validated[AccountAuthentication] match {
//            case Success(a) => {
//              accounts.closeAccount(a)
//            }
//            case Failure(e) => {
//              monitor.count(".accounts.get.failure")
//              Future.sync(failure(e.message))
//            }
//          }
//        }
//      }).map[HttpResponse[JValue]](x => collapseToHttpResponse(x.map(_.serialize)))
//    }
//  }

  class LegacyGetAccountHandler(config: BillingConfiguration, monitor: HealthMonitor) extends AccountAccessHandler(config) {

    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
      (request.content match {
        case None => {
          monitor.count(".accounts.get.failure")
          Future.sync[Validation[String, Account]](failure("Missing request content"))
        }
        case Some(x) => x.flatMap[Validation[String, Account]] {
          _.validated[AccountAuthentication] match {
            case Success(a) => accounts.getAccount(a)
            case Failure(e) => {
              monitor.count(".accounts.get.failure")
              Future.sync(failure(e.message))
            }
          }
        }
      }).map[HttpResponse[JValue]](x => collapseToHttpResponse(x.map(_.serialize)))
    }
  }

  class AccountAuditHandler(config: BillingConfiguration, monitor: HealthMonitor) extends HttpServiceHandler[Future[JValue], Future[HttpResponse[JValue]]] {

    private val accounts = config.accounts

    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
      val results = accounts.audit()
      Future.sync(JString("Audit complete.\n").ok)
    }

  }

  class AccountAssessmentHandler(config: BillingConfiguration, monitor: HealthMonitor) extends HttpServiceHandler[Future[JValue], Future[HttpResponse[JValue]]] {

    private val accounts = config.accounts

    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
      config.sendEmail("accounts@reportgrid.com", Array("nick@reportgrid.com"), Array(), Array(), "Assessment ran - " + new DateTime(), "<Assessment results still needs to be implemented.>")
      val token: Option[String] = request.parameters.get('token)
      token.map { t =>
        if (t.equals(Token.Root.tokenId)) {
          accounts.assessment()
          Future.sync(HttpResponse(content = Some[JValue](JString("Assessment complete."))))
        } else {
          Future.sync(HttpResponse(HttpStatus(401, "Invalid token."), content = Some[JValue](JString("Invalid token."))))
        }
      }.getOrElse(Future.sync(HttpResponse(HttpStatus(401, "Token required."), content = Some[JValue](JString("Token required.")))))
    }
  }

  class RequireHeaderParameter[T, S](headerParameter: String, errorMessage: String, val delegate: HttpService[T, S]) extends DelegatingService[T, S, T, S] {

    val metadata = None

    def service = (r: HttpRequest[T]) => {
      val p = r.headers.get(headerParameter)
      p match {
        case Some(v) => delegate.service(r)
        case None => Failure(DispatchError(Unauthorized, errorMessage))
      }
    }
  }

  def headerParameterRequired[T, S](parameter: String, error: String): HttpService[T, S] => HttpService[T, S] = {
    in => new RequireHeaderParameter[T, S](parameter, error, in)
  }

  case class BillingConfiguration(
    naccounts: PublicAccounts,
    accounts: Accounts,
    mailer: Mailer) {

    private val fromAddress = "accounts@reportgrid.com"
    private val userSignupBccList = Array("nick@reportgrid.com")
    private val internalSignupBccList = Array("nick@reportgrid.com", "jason@reportgrid.com")
    private val failedSignupToList = Array("nick@reportgrid.com")

    private val accountConfirmationContent = """
Welcome to ReportGrid
  

Here is your account information:
      
id: %s
production token: %s
development token: %s

Just getting started?

Our developer page has everything you need to get started. Find API documentation, an interactive console and client libraries.

http://reportgrid.com/developer.html


Need help?
      
Checkout the resources on our support page or feel free to contact us.
  
http://reportgrid.com/support.html
support@reportgrid.com


Thanks,
      
The ReportGrid Team
    """

    def sendSignupSuccess(a: Account) {
      sendUserSignupEmail(Array(a.id.email), accountConfirmationContent.format(a.id.email, a.id.tokens.production, a.id.tokens.development.getOrElse("<NA>")))
      sendInternalSignupEmail(a)
    }

    def sendSignupFailure(c: Option[CreateAccount], e: String) {
      sendFailedSignupEmail(c, e)
    }

    private def sendUserSignupEmail(to: Array[String], content: String) = sendEmail(
      fromAddress,
      to,
      Array(),
      userSignupBccList,
      "Welcome to ReportGrid",
      content)

    private def sendInternalSignupEmail(a: Account) = sendEmail(
      fromAddress,
      internalSignupBccList,
      Array(),
      Array(),
      "New User Signup [" + a.contact.company + "|" + a.service.planId + "|" + a.id.email + "]",
      accountToMessageBody(a))

    private def accountToMessageBody(a: Account): String = {
      Printer.pretty(Printer.render(a.serialize))
    }

    private def sendFailedSignupEmail(c: Option[CreateAccount], e: String) = sendEmail(
      fromAddress,
      failedSignupToList,
      Array(),
      Array(),
      "Failed User Signup [" + e + "]",
      failedSignupMessageBody(c, e))

    private def failedSignupMessageBody(c: Option[CreateAccount], e: String): String = {
      "Signup Error: " + e + "\n\n" + Printer.pretty(Printer.render(c.serialize))
    }

    def addQuery(client: HttpClient[ByteChunk], key: String, value: String): HttpClient[ByteChunk] = {
      client.query(key, value)
    }

    def addQuery(client: HttpClient[ByteChunk], kv: (String, Option[String])): HttpClient[ByteChunk] = {
      kv._2.map(client.query(kv._1, _)).getOrElse(client)
    }

    def addQueryList(client: HttpClient[ByteChunk], kv: (String, Option[Array[String]])): HttpClient[ByteChunk] = {
      val vals: List[(String, Option[String])] = kv._2.getOrElse(Array()).toList.map(v => (kv._1 + "[]", Some(v)))
      vals.foldLeft(client)(addQuery)
    }

    def sendEmail(from: String, to: Array[String], cc: Array[String], bcc: Array[String], title: String, content: String) {
      mailer.sendEmail(from, to, cc, bcc, title, content)
    }

    def shutdown(): Unit = Unit

    def developerCredit: Int = 25000
  }

}