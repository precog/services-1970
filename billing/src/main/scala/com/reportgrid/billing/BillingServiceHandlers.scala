package com.reportgrid.billing

import java.util.UUID
import java.security.SecureRandom

import scala.collection.JavaConverters._

import scalaz._
import scalaz.Scalaz._
import scalaz.Validation._

import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.codec.binary.Hex
import com.braintreegateway._

import com.reportgrid.analytics._
import com.reportgrid.billing.braintree._

import blueeyes.persistence.mongo.Database
import blueeyes.persistence.mongo._
import blueeyes.concurrent.Future
import blueeyes.core.http.HttpStatusCodes.Unauthorized
import blueeyes.core.http.HttpRequest
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser
import blueeyes.core.service._
import blueeyes.core.data._
import blueeyes.core.data.BijectionsChunkJson._

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.Extractor._
import Extractor.Error
import blueeyes.json.xschema.DefaultSerialization._

import SerializationHelpers._

abstract class TokenGenerator {
  def newToken(path: String): Future[Validation[String, String]]
  def deleteToken(token: String): Future[Unit]
}

class MockTokenGenerator extends TokenGenerator {
  override def newToken(path: String) = {
    Future.sync(Success(UUID.randomUUID().toString().toUpperCase()))
  }

  override def deleteToken(token: String) = {
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

  override def deleteToken(token: String) = {
    client.
      query("tokenId", rootToken).
      contentType[ByteChunk](application / json).
      delete[JValue](rootUrl + token)
  }.map(_ => Unit)
}

object BillingServiceHandlers {

  private def jerror(message: String): JValue = JString(message)
  private def jerror(error: Error): JValue = jerror(error.message)

  private def collapseToHttpResponse(v: Validation[String, JValue]): HttpResponse[JValue] = v match {
    case Success(jv) => HttpResponse[JValue](content = Some(jv))
    case Failure(e) => HttpResponse(HttpStatusCodes.BadRequest, content = Some(e))
  }

  class CreateAccountHandler(config: BillingConfiguration) extends HttpServiceHandler[Future[JValue], Future[HttpResponse[JValue]]] {

    private val accounts = config.accounts
    private val mailer = config.mailer

    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
      (request.content match {
        case None => Future.sync[Validation[String, Account]](failure("Missing request content"))
        case Some(js) => js.flatMap[Validation[String, Account]] {
          _.validated[CreateAccount] match {
            case Success(ca) => accounts.create(ca)
            case Failure(e) => Future.sync(failure(e.message))
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

  class CloseAccountHandler(config: BillingConfiguration) extends HttpServiceHandler[Future[JValue], Future[HttpResponse[JValue]]] {
    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
      (request.content match {
        case None => Future.sync[JValue](jerror("outer bang"))
        case Some(x) => x.map[JValue] {
          _.validated[AccountAction].fold(jerror, aa => TestAccounts.testAccount.serialize)
        }
      }).map(jval => HttpResponse[JValue](content = Some(jval)))
    }
  }

  // Notes
  // * requires id, token and password (respec all or allow partial structure?)
  // * how to handle billing implications?
  // * email confirmation of service changes?
  class UpdateAccountHandler(config: BillingConfiguration) extends HttpServiceHandler[Future[JValue], Future[HttpResponse[JValue]]] {
    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
      (request.content match {
        case None => Future.sync[JValue](jerror("outer bang"))
        case Some(x) => x.map[JValue] {
          _.validated[UpdateAccount].fold(jerror, ua => TestAccounts.testAccount.serialize)
        }
      }).map(jval => HttpResponse[JValue](content = Some(jval)))
    }
  }

  // Notes
  // * requires id, token (Master token?) and usage measure
  // * investigate external billing service to ensure usage is clearly communicated
  // ** if not this may require the tracking of aggregate usage in our billing system (ich!)
  class AccountUsageHandler(config: BillingConfiguration) extends HttpServiceHandler[Future[JValue], Future[HttpResponse[JValue]]] {
    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
      (request.content match {
        case None => Future.sync[JValue](jerror("outer bang"))
        case Some(x) => x.map[JValue] {
          _.validated[AccountAction].fold(jerror, x => JString("Need to implement this"))
        }
      }).map(jval => HttpResponse[JValue](content = Some(jval)))
    }
  }

  class GetAccountHandler(config: BillingConfiguration) extends HttpServiceHandler[Future[JValue], Future[HttpResponse[JValue]]] {

    private val accounts = config.accounts

    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
      (request.content match {
        case None => Future.sync[Validation[String, Account]](failure("Missing request content"))
        case Some(x) => x.flatMap[Validation[String, Account]] {
          _.validated[AccountAction] match {
            case Success(a) => getAccount(a)
            case Failure(e) => Future.sync(failure(e.message))
          }
        }
      }).map[HttpResponse[JValue]](x => collapseToHttpResponse(x.map(_.serialize)))
    }

    def getAccount(request: AccountAction): Future[Validation[String, Account]] = {
      val token = request.accountToken
      val email = request.email

      val account = if (token.isDefined && token.get.trim.size > 0) {
        accounts.findByToken(token.get)
      } else if (email.isDefined && email.get.trim.size > 0) {
        accounts.findByEmail(email.get)
      } else {
        Future.sync(Failure("You must provide a valid token or email."))
      }
      account.map { oa =>
        oa.flatMap { a =>
          if (PasswordHash.checkSaltedHash(request.password, a.id.passwordHash))
            Success(a)
          else
            Failure("You must provide valid identification and authentication information.")
        }
      }
    }
  }

  class AccountAuditHandler(config: BillingConfiguration) extends HttpServiceHandler[Future[JValue], Future[HttpResponse[JValue]]] {

    private val accounts = config.accounts

    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
      val results = accounts.audit()
      Future.sync(JString("Audit complete.\n").ok)
      //      val plans = config.billingService.findPlans
      //      config.findAccounts().map { (accounts => 
      //        {
      //          val validAccounts = accounts.flatMap{
      //            case Success(a) => a :: Nil
      //            case Failure(_) => Nil
      //          }
      //          val creditResults = adjustAccountCredit(plans, validAccounts)
      //          val results = runAudit(accounts)
      //          JString("Audit complete.\n" + auditReport(results)).ok
      //        })
      //      }
    }

    def adjustAccountCredit(plans: List[Plan], accounts: List[Account]): AdjustCreditResults = {
      accounts.foldLeft(new AdjustCreditResults())(adjustCredits(plans))
    }

    private val monthsPerYear: Float = 12
    private val daysPerYear: Float = 365
    private val averageDaysPerMonth: Float = 365 / 12

    def adjustCredits(plans: List[Plan])(results: AdjustCreditResults, account: Account): AdjustCreditResults = {

      val planRate = 50 // Get from plan list
      val dailyRate = planRate / averageDaysPerMonth

      val daysToBeBilled = 1 // compare today and last bill adjustment date
      val newCredit = math.max(0, account.service.credit - daysToBeBilled * dailyRate)

      // if account disable skip
      // if credit == 0
      //   if billing info exists 
      //     start subscription
      //     update account credit and last bill adjustment
      //     send notification
      //   else
      //     terminate account
      //     send notification
      // else
      //   if(credit / dailyRate < 7) {
      //     send email once
      //   else
      //     do nothing

      results
    }

    class AdjustCreditResults() {

    }

    def auditReport(results: OldAuditResults): String = {
      "Audit results: (" + results.passed + "/" + results.checked + ")\n" +
        "Billing: (" + results.activeBillingInfo + ") Subscriptions: (" + results.activeSubscriptions + ")\n" +
        "Errors:\n" +
        (results.errors.foldLeft("")(_ + "," + _))
    }

    def runAudit(accounts: List[Validation[String, Account]]): OldAuditResults = {
      accounts.foldLeft(new OldAuditResults())(process)
    }

    def process(results: OldAuditResults, account: Validation[String, Account]): OldAuditResults = {
      account match {
        case Success(a) => {
          if (accountValid(a)) {
            results.passedChecks(a.hasBillingInfo, a.hasSubscription)
          } else {
            results.failedChecks(a.hasBillingInfo, a.hasSubscription)
          }
        }
        case Failure(e) => {
          results.checkError(e)
        }
      }
    }

    def accountValid(account: Account): Boolean = {
      valuesInExpectedRange(account) &&
        billingAccountInSync(account)
    }

    def valuesInExpectedRange(account: Account): Boolean = {
      account.service.credit >= 0 && account.service.usage >= 0
    }

    def billingAccountInSync(account: Account): Boolean = {
      if (account.service.credit == 0 && account.service.status == AccountStatus.ACTIVE) {
        account.billing.isDefined
      } else {
        true
      }
    }

    class OldAuditResults(val checked: Int, val passed: Int, val activeBillingInfo: Int, val activeSubscriptions: Int, val errors: List[String]) {
      def this() = this(0, 0, 0, 0, Nil)

      def checkError(error: String): OldAuditResults = new OldAuditResults(checked + 1, passed, activeBillingInfo, activeSubscriptions, error :: errors)
      def passedChecks(hasBilling: Boolean, hasSubscription: Boolean): OldAuditResults = {
        new OldAuditResults(
          checked + 1,
          passed + 1,
          incBilling(hasBilling),
          incSubscriptions(hasSubscription),
          errors)
      }

      def failedChecks(hasBilling: Boolean, hasSubscription: Boolean): OldAuditResults = {
        new OldAuditResults(
          checked + 1,
          passed,
          incBilling(hasBilling),
          incSubscriptions(hasSubscription),
          errors)
      }

      private def incBilling(hasBilling: Boolean): Int = if (hasBilling) activeBillingInfo + 1 else activeBillingInfo
      private def incSubscriptions(hasSubscription: Boolean): Int = if (hasSubscription) activeSubscriptions + 1 else activeSubscriptions
    }

    // Things that should be done
    // - based on service plan decrement credit and initiate subscription if no more credit
    // - flag plans without credit or active payment as disabled
    // - need to thing more about usage monitoring stuff...

    // Reports
    // - plans in credit period (number, possibly break into days spent/remaining)
    // - active plans
    // - disable or otherwise problemattic plans

    // Sanity checks

    // Things that shouldn't be so
    // - accounts with negative credit
    // - accounts with negative usage
    // - accounts with zero credit and no matching billing account
    // - billing account with 0 or more than 1 credit card
    // - billing account with more than 1 subscription
    // - duplicate emails/accountTokens in either accounts or billing
  }

  class AccountAssessmentHandler(config: BillingConfiguration) extends HttpServiceHandler[Future[JValue], Future[HttpResponse[JValue]]] {

    private val accounts = config.accounts

    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
      accounts.assessment()
      Future.sync(HttpResponse(content = Some(JString("Hello"))))
    }
  }

  // Notes
  // * requires id
  // * investigate external billing service to ensure usage is clearly communicated
  // ** if not this may require the tracking of aggregate usage in our billing system (ich!)
  //  class IsAccountActive(config: BillingConfiguration) extends HttpServiceHandler[Future[JValue], String => Future[HttpResponse[JValue]]] {
  //    def apply(request: HttpRequest[Future[JValue]]): String => Future[HttpResponse[JValue]] = accountId => {
  //      (request.content match {
  //        case None => Future.sync[JValue](jerror("outer bang"))
  //        case Some(x) => x.map[JValue] {
  //          _.validated[UserSignup].fold(jerror, _.serialize)
  //        }
  //      }).map(jval => HttpResponse[JValue](content = Some(jval)))
  //    }
  //  }

  class StaticJValue extends HttpServiceHandler[Future[JValue], Future[HttpResponse[JValue]]] {
    def apply(request: HttpRequest[Future[JValue]]): Future[HttpResponse[JValue]] = {
      println("Static response foo")
      Future.sync(HttpResponse[JValue](content = Some(JString("Static response"))))
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
    accounts: Accounts,
    mailer: Mailer) {
    //    tokenGenerator: TokenGenerator,
    //    database: Database,
    //    accountsCollection: String,
    //    billingService: BraintreeService) {

    def shutdown(): Unit = Unit

    def developerCredit: Int = 25000
  }

  // Used for get and close account operations
  case class AccountAction(
    email: Option[String],
    accountToken: Option[String],
    password: String)

  trait AccountActionSerialization {
    implicit val AccountActionDecomposer: Decomposer[AccountAction] = new Decomposer[AccountAction] {
      override def decompose(action: AccountAction): JValue = JObject(
        List(
          JField("email", action.email.serialize),
          JField("accountToken", action.accountToken.serialize),
          JField("password", action.password.serialize)).filter(fieldHasValue))
    }

    implicit val AccountActionExtractor: Extractor[AccountAction] = new Extractor[AccountAction] with ValidatedExtraction[AccountAction] {
      override def validated(obj: JValue): Validation[Error, AccountAction] = (
        (obj \ "email").validated[Option[String]] |@|
        (obj \ "accountToken").validated[Option[String]] |@|
        (obj \ "password").validated[String]).apply(AccountAction(_, _, _))
    }
  }

  object AccountAction extends AccountActionSerialization

  case class UpdateAccount(
    email: Option[String],
    company: Option[String],
    website: Option[String],
    planId: Option[String],
    passwordHash: String,
    newPasswordHash: Option[String],
    accountToken: String,
    billing: Option[BillingInformation])

  trait UpdateAccountSerialization {

    // Decomposer not required because instances only come into the service

    implicit val UpdateAccountExtractor: Extractor[UpdateAccount] = new Extractor[UpdateAccount] with ValidatedExtraction[UpdateAccount] {
      override def validated(obj: JValue): Validation[Error, UpdateAccount] = (
        (obj \ "email").validated[Option[String]] |@|
        (obj \ "company").validated[Option[String]] |@|
        (obj \ "website").validated[Option[String]] |@|
        (obj \ "planId").validated[Option[String]] |@|
        (obj \ "passwordHash").validated[String] |@|
        (obj \ "newPasswordHash").validated[Option[String]] |@|
        (obj \ "accountToken").validated[String] |@|
        (obj \ "billing").validated[Option[BillingInformation]]).apply(UpdateAccount(_, _, _, _, _, _, _, _))
    }
  }

  object UpdateAccount extends UpdateAccountSerialization

}