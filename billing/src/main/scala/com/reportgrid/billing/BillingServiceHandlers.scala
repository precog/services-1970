package com.reportgrid.billing

import java.util.UUID
import java.security.SecureRandom

import scala.collection.JavaConverters._

import scalaz._
import scalaz.Scalaz._
import scalaz.Validation._

import org.joda.time.DateTime
import org.joda.time.DateMidnight
import org.joda.time.Minutes
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
import blueeyes.core.http.MimeTypes._
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

trait UsageClient {
  def apiCalls(tokenId: String, start: DateMidnight, finish: DateMidnight): Future[Long]
  def apiCalls(tokenId: String, path: String, start: DateMidnight, finish: DateMidnight): Future[Long]
}

class RealUsageClient(client: HttpClient[ByteChunk], baseUrl: String) extends UsageClient {
    
  def timeBoundedUsageSums(tokenId: String, path: String, start: DateMidnight, end: DateMidnight): Future[HttpResponse[JValue]] = {
    val c = client.contentType[JValue](application/(MimeTypes.json))
                                   .query("tokenId", tokenId)
                                   .query("start", start.getMillis.toString)
                                   .query("end", end.getMillis.toString)

    c.get[JValue](baseUrl + "vfs/" + path + ".stored.count/series/day/sums")
  }

  def reduceUsageSums(jval: JValue): Long = {
    jval match {
      case JArray(Nil) => 0l
      case JArray(l)   => l.map {
        case JArray(ts :: JDouble(c) :: Nil ) => c.toLong
        case JArray(ts :: v :: Nil)         => 0l
        case _                              => scala.sys.error("Unexpected series result format")
      }.reduce(_ + _)
      case _           => scala.sys.error("Error parsing usage count.")
    }
  }
    
  def apiCalls(tokenId: String, start: DateMidnight, finish: DateMidnight): Future[Long] = {
    apiCalls(tokenId, "", start, finish)
  }
  
  def apiCalls(tokenId: String, path: String, start: DateMidnight, finish: DateMidnight): Future[Long] = {
    val result = timeBoundedUsageSums(tokenId, path, start, finish)
    result.map(_.content.map(reduceUsageSums).getOrElse(0))
  }
}

class MockUsageClient extends UsageClient {
  
  var default: Option[Long] = None
  
  def apiCalls(tokenId: String, start: DateMidnight, finish: DateMidnight): Future[Long] = {
    apiCalls(tokenId, "", start, finish)
  }
  
  def apiCalls(tokenId: String, path: String, start: DateMidnight, finish: DateMidnight): Future[Long] = {
    val minutes = Minutes.minutesBetween(start, finish)
    Future.sync(default.getOrElse( minutes.getMinutes )) 
  }
}

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
  val defaultRollup = 0

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
                "tags" : %d,
                "rollup" : %d
            }
          }
        """.format(path, defaultOrder, defaultLimit, defaultDepth, defaultTags, defaultRollup)))
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
    val accounts = config.publicAccounts
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
    r.flatMap {
      _.map(f) match {
        case Success(fva) => fva
        case f @ Failure(s) => Future.sync(Failure(s))
      }
    }
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
            case Failure(e) => Future.sync(Failure(e.message))
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
                accounts.getAccountInformation(a).orElse { ot =>
                  Failure(ot.map(t =>
                    { t.printStackTrace(System.out); "Caught exception." }).getOrElse("Shouldn't happen"))
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

  class UsageAccountingHandler(config: BillingConfiguration, monitor: HealthMonitor) extends AccountAccessHandler(config) {

    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
      val token: Option[String] = request.parameters.get('token)
      token.map { t =>
        if (t.equals(Token.Root.tokenId)) {
          accounts.usageAccounting()
          config.sendAdminNotification("Usage accounting ran - " + new DateTime(), "<Usage accounting results still needs to be implemented.>")
          Future.sync(HttpResponse(content = Some[JValue](JString("Usage accounting complete."))))
        } else {
          Future.sync(HttpResponse(HttpStatus(401, "Invalid token."), content = Some[JValue](JString("Invalid token."))))
        }
      }.getOrElse(Future.sync(HttpResponse(HttpStatus(401, "Token required."), content = Some[JValue](JString("Token required.")))))
    }
  }

  class CreditAccountingHandler(config: BillingConfiguration, monitor: HealthMonitor) extends AccountAccessHandler(config) {

    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
      val token: Option[String] = request.parameters.get('token)
      token.map { t =>
        if (t.equals(Token.Root.tokenId)) {
          accounts.creditAccounting()
          config.sendAdminNotification("Credit accounting ran - " + new DateTime(), "<Credit accounting results still needs to be implemented.>")
          Future.sync(HttpResponse(content = Some[JValue](JString("Credit accounting complete."))))
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

  val fromAddress = "accounts@reportgrid.com"
  val userSignupBccList = Array("nick@reportgrid.com")
  val internalSignupBccList = Array("nick@reportgrid.com", "jason@reportgrid.com")
  val failedSignupToList = Array("nick@reportgrid.com")

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

  class AdminNotification(s: String, b: String) extends Notification(fromAddress, internalSignupBccList, Array(), Array()) {
    def subject = s
    def body = b
  }    
    
  class SignupConfirmation(a: Account) extends Notification(fromAddress, Array(a.id.email), Array(), userSignupBccList) {
    def subject = "Welcome to ReportGrid"
    def body = accountConfirmationContent.format(a.id.email, a.id.tokens.production, a.id.tokens.development.getOrElse("<NA>"))
  }
  
  class InternalSignupDetail(a: Account) extends Notification(fromAddress, internalSignupBccList, Array(), Array()) {
    def subject = "New User Signup [" + a.contact.company + "|" + a.service.planId + "|" + a.id.email + "]"
    def body = Printer.pretty(Printer.render(a.serialize))
  }
  
  class FailedSignupDetail(c: Option[CreateAccount], e: String) extends Notification(fromAddress, internalSignupBccList, Array(), Array()) {
    def subject = "Failed User Signup [" + e + "]"
    def body = "Signup Error: " + e + "\n\n" + Printer.pretty(Printer.render(c.serialize))
  }

  case class BillingConfiguration(
    publicAccounts: PublicAccounts,
    notifications: NotificationSender) {

    def sendSignupSuccess(a: Account) {
      notifications.send(new SignupConfirmation(a))
      notifications.send(new InternalSignupDetail(a))
    }

    def sendSignupFailure(c: Option[CreateAccount], e: String) {
      notifications.send(new FailedSignupDetail(c, e))
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

    def sendAdminNotification(subject: String, body: String) {
      notifications.send(new AdminNotification(subject, body))
    }
    
    def shutdown(): Unit = Unit

    def developerCredit: Int = 25000
  }

}
