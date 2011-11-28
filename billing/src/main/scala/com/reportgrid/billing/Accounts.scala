package com.reportgrid.billing

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import scalaz.{ Validation, Success, Failure, Semigroup }
import scalaz.Scalaz._

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.DateMidnight
import org.joda.time.Days

import com.braintreegateway._
import com.reportgrid.billing.braintree.BraintreeService
import com.reportgrid.billing.braintree.BraintreeUtils._

import net.lag.configgy._

import blueeyes.concurrent.Future
import blueeyes.json.xschema._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._
import blueeyes.persistence.mongo.MongoImplicits._
import blueeyes.persistence.mongo.MongoQueryBuilder
import blueeyes.persistence.mongo.Database
import blueeyes.persistence.mongo.MongoSelectQuery

import Extractor.Error

import SerializationHelpers._

object FutureValidationHelpers {
  def fvApply[E: Semigroup, A, B](r: Future[Validation[E, A]], f: A => Future[Validation[E, B]]): Future[Validation[E, B]] = {
    r.flatMap {
      _.map(f) match {
        case Success(fva) => fva
        case f @ Failure(s) => Future.sync(Failure(s))
      }
    }
  }

  def fvJoin[E: Semigroup, A, B](fa: Future[Validation[E, A]], fb: Future[Validation[E, B]]): Future[Validation[E, (A, B)]] = {
    (fa join fb).map {
      case (Success(a), Success(b)) => Success((a, b))
      case (Success(_), Failure(e)) => Failure(e)
      case (Failure(e), Success(_)) => Failure(e)
      case (Failure(e1), Failure(e2)) => Failure((e1: E) ⊹ (e2))
    }
  }

}

trait PublicAccounts {
  type FV[E, A] = Future[Validation[E, A]]

  def openAccount(openAccount: CreateAccount): FV[String, Account]
  def getAccount(auth: AccountAuthentication): FV[String, Account]
  def closeAccount(auth: AccountAuthentication): FV[String, Account]

  def getAccountInformation(auth: AccountAuthentication): FV[String, AccountInformation]
  def updateAccountInformation(update: UpdateAccount): FV[String, AccountInformation]

  def getBillingInformation(auth: AccountAuthentication): FV[String, Option[BillingInformation]]
  def updateBillingInformation(update: UpdateBilling): FV[String, BillingInformation]
  def removeBillingInformation(authetnication: AccountAuthentication): FV[String, Option[BillingInformation]]

  def creditAccounting(): FV[String, CreditAccountingResults]
  def usageAccounting(): FV[String, UsageAccountingResults]
}

trait NotificationSender {
  def send(n: Notification): Future[Unit]
}

class MockNotificationSender extends NotificationSender {

  val notifications = ListBuffer[Notification]()

  def send(n: Notification) = {
    notifications += n
    Future.sync(Unit)
  }
}

class MailerNotificationSender(mailer: Mailer) extends NotificationSender {
  def send(n: Notification) = {
    mailer.sendEmail(n.from, n.to, n.cc, n.bcc, n.subject, n.body).toUnit
  }
}

abstract case class Notification(from: String, to: Array[String], cc: Array[String], bcc: Array[String]) {
  def subject: String
  def body: String
  
  override def toString(): String = 
    """
Subject: %s
Body:
%s
    """.format(subject, body)
}

class UpgradeWarning(from: String, to: Array[String], cc: Array[String], bcc: Array[String]) extends Notification(from, to, cc, bcc) {
  def subject = "ReportGrid: Automatic account upgrade warning"
  def body = 
"""
At your current usage rate we anticipate you will exceed your monthly usage quota. 
    
If you exceed your monthly quota you will be automatically upgraded to the next level of service.
    
This is just a warning no changes have been made to your account.
    
The ReportGrid Team
support@reportgrid.com
"""
}

class UpgradeConfirmation(from: String, to: Array[String], cc: Array[String], bcc: Array[String]) extends Notification(from, to, cc, bcc) {
  def subject = "ReprotGrid: Usage quota exceeded account upgraded"
  def body =
"""
Your usage exceeded your current plan quota.
    
You have been upgrade to the next level of service.
    
The ReportGrid Team
support@reportgrid.com
"""
}

class NoUpgradeWarning(from: String, to: Array[String], cc: Array[String], bcc: Array[String]) extends Notification(from, to, cc, bcc) {
  def subject = "ReportGrid: Usage quota exceeded account now in metered usage"
  def body =
"""
Your usage exceeded your current plan quota.
    
You are currently in our highest tier, your usage will now be billed at a metered rate."
    
The ReportGrid Team
support@reportgrid.com
"""
}

class PrivateAccounts(
  config: ConfigMap,
  accountStore: AccountInformationStore,
  billingStore: BillingInformationStore,
  usageClient: UsageClient,
  notifications: NotificationSender,
  tokens: TokenGenerator) extends PublicAccounts {

  import FutureValidationHelpers._

  def credits = config.getConfigMap("credits")

  def openAccount(openAccount: CreateAccount): FV[String, Account] = {

    val errs = validateCreate(openAccount)
    errs.map[FV[String, Account]](e =>
      Future.sync(Failure(e)))
      .getOrElse {
        val existingAccount = accountStore.getByEmail(openAccount.email)

        val credit = openAccount.planCreditOption.flatMap { creditProgram =>
          credits.map { c =>
            val r = c.getInt(creditProgram, 0)
            r
          }
        }.getOrElse(0)

        existingAccount.flatMap[Validation[String, Account]] {
          case Success(a) => Future.sync(Failure("An account associated with this email already exists."))
          case Failure(_) => {
            val accountTokens = newAccountTokens(toPath(openAccount.email))

            accountTokens.flatMap[Validation[String, Account]] {
              case Success(ats) => {
                val billingData = establishBilling(openAccount, ats.master, credit > 0)

                billingData.flatMap {
                  case Success(bd) => {
                    val accountInfo = createAccountInformation(openAccount, bd, ats, credit)
                    accountInfo.map[Validation[String, Account]] {
                      case Success(ai) => {
                        Success(buildAccount(bd, ai))
                      }
                      case Failure(e) => {
                        undoBilling(ats.master)
                        undoNewTokens(ats)
                        Failure(e)
                      }
                    }
                  }
                  case Failure(e) => {
                    undoNewTokens(ats)
                    Future.sync(Failure[String, Account](e))
                  }
                }
              }
              case Failure(e) => {
                Future.sync(Failure[String, Account](e))
              }
            }
          }
        }
      }
  }

  def createAccountInformation(create: CreateAccount, billingData: BillingData, tokens: AccountTokens, credit: Int): FV[String, AccountInformation] = {
    val created = new DateTime(DateTimeZone.UTC)
    val today = created.toDateMidnight().toDateTime(DateTimeZone.UTC)
    val aid = AccountId(create.email, tokens, PasswordHash.saltedHash(create.password))
    val srv = ServiceInformation(
      create.planId,
      created,
      credit,
      today,
      0,
      today,
      None,
      None,
      AccountStatus.ACTIVE,
      None,
      billingDay(today),
      billingData.subscriptionId)

    val accountInfo = AccountInformation(aid, create.contact, srv)

    val futureResult = accountStore.create(accountInfo)

    futureResult.map[Validation[String, AccountInformation]] { _ =>
      Success(accountInfo)
    }.orElse { ot: Option[Throwable] =>
      ot.map { t: Throwable =>
        val message = t.getMessage
        if (message.startsWith("E11000")) {
          Failure("An account associated with this email already exists.")
        } else {
          Failure(message)
        }
      }.getOrElse {
        Failure("Error initializing account.")
      }
    }
  }

  def establishBilling(create: CreateAccount, token: String, hasCredit: Boolean): FV[String, BillingData] = {
    create.billing.map[FV[String, BillingData]](bi => {
      fvApply(billingStore.create(token, create.email, create.contact, bi), (nbi: BillingInformation) => {
        if (!hasCredit) {
          val subs = billingStore.startSubscription(token, create.planId)
          fvApply(subs, (s: String) => {
            Future.sync(Success(BillingData(Some(nbi), Some(s))))
          })
        } else {
          Future.sync(Success(BillingData(Some(nbi), None)))
        }
      })
    }).getOrElse(
      if (hasCredit) {
        Future.sync(Success(BillingData(None, None)))
      } else {
        Future.sync(Failure("Unable to create account without account credit or billing information."))
      })
  }

  def buildAccount(billingData: BillingData, accountInfo: AccountInformation): Account = {
    val service = accountInfo.service
    val newService = service.copy(subscriptionId = billingData.subscriptionId)

    ValueAccount(
      accountInfo.id,
      accountInfo.contact,
      newService,
      billingData.info)
  }

  def undoBilling(token: String): Unit = {
    billingStore.removeByToken(token)
  }

  private def toPath(email: String): String = {
    val parts = email.split("[@.]")
    val sanitizedParts = parts.map(sanitize)
    "/" + sanitizedParts.reduceLeft((a, b) => b + "_" + a)
  }

  private def sanitize(s: String): String = {
    s.replaceAll("\\W", "_")
  }

  def newAccountTokens(path: String): FV[String, AccountTokens] = {
    val master = newToken(path)
    val tokens: FV[String, (((String, String), String), String)] = fvApply(newToken(path), (m: String) => {
      val prod = newChildToken(m, "/prod/")
      val tracking: FV[String, String] = Future.sync(Success("tracking-tbd"))
      val dev = newChildToken(m, "/dev/")
      fvJoin(fvJoin(fvJoin(master, tracking), prod), dev)
    });

    tokens.map(_.map { t =>
      {
        AccountTokens(t._1._1._1, t._1._1._2, t._1._2, Some(t._2))
      }
    })
  }

  def newChildToken(parent: String, path: String): FV[String, String] = {
    tokens.newChildToken(parent, path)
  }

  def newToken(path: String): Future[Validation[String, String]] = {
    tokens.newToken(path)
  }

  def undoNewTokens(accountTokens: AccountTokens): Unit = {
    accountTokens.development.foreach(tokens.deleteToken(_))
    tokens.deleteToken(accountTokens.production)
    tokens.deleteToken(accountTokens.tracking)
    tokens.deleteToken(accountTokens.master)
  }

  private def validateCreate(create: CreateAccount): Option[String] = {
    validateOther(create).orElse(validateBilling(create.billing))
  }

  private def validateOther(c: CreateAccount): Option[String] = {
    if (!validEmailAddress(c.email)) {
      Some("Invalid email address: " + c.email)
    } else if (c.password.length < 5) {
      Some("Password must be at least five characters long.")
    } else if (!planAvailable(c.planId)) {
      Some("The selected plan (" + c.planId + ") is not available.")
    } else {
      None
    }
  }

  private def validateBilling(ob: Option[BillingInformation]): Option[String] = {
    ob match {
      case Some(b) if b.cardholder.trim().size == 0 => Some("Cardholder name required.")
      case Some(b) if b.billingPostalCode.trim().size == 0 => Some("Postal code required.")
      case _ => None
    }
  }

  private val emailPattern = "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,4}".r
  private def validEmailAddress(e: String): Boolean = {
    emailPattern.pattern.matcher(e).matches()
  }

  private def planAvailable(p: String): Boolean = {
    val plans = billingStore.findPlans()
    plans.any(plan => plan.getId() == p)
  }

  def getAccount(auth: AccountAuthentication): FV[String, Account] = {
    getAuthenticatedAccount(auth)
  }

  def closeAccount(auth: AccountAuthentication): FV[String, Account] = {
    fvApply(getAuthenticatedAccount(auth), (a: Account) => {
      val tokenDisabled = tokens.disableToken(a.id.tokens.master)
      val mongoAccountDisabled = accountStore.disableByToken(a.id.tokens.master)
      val billingAccountDisabled = billingStore.removeByToken(a.id.tokens.master)
      val returnAccount: FV[String, Account] = Future.sync(Success(a))

      tokenDisabled
        .then(mongoAccountDisabled)
        .then(billingAccountDisabled)
        .then(returnAccount)
    })
  }

  def getAccountInformation(auth: AccountAuthentication): FV[String, AccountInformation] = {
    getAuthenticatedAccount(auth).map(_.map(_.asAccountInformation))
  }

  def updateAccountInformation(update: UpdateAccount): FV[String, AccountInformation] = {
    fvApply(getAuthenticatedAccount(update.auth), (a: Account) => {
      fvApply(combineAccountUpdates(a.asAccountInformation, update), (ai: AccountInformation) => {
        val billingData = updateBillingIfRequired(a, ai)
        fvApply(billingData, (bd: BillingData) => {
          val nai = if (bd.subscriptionId.isDefined) {
            ai.copy(service = ai.service.copy(subscriptionId = bd.subscriptionId))
          } else {
            ai
          }
          accountStore.update(nai)
        })
      })
    })
  }

  private def updateBillingIfRequired(currentAccount: Account, accountUpdateInfo: AccountInformation): FV[String, BillingData] = {
    val default: FV[String, Option[BillingInformation]] = Future.sync(Success(currentAccount.billing))
    val contactUpdates: FV[String, Option[BillingInformation]] = if(
       accountUpdateInfo.contact != currentAccount.contact || 
       accountUpdateInfo.id.email != currentAccount.id.email) {
      
      currentAccount.billing.map {_ => {
        billingStore.update(currentAccount.id.tokens.master, accountUpdateInfo.id.email, accountUpdateInfo.contact)
                    .map(_.map(Some(_))): FV[String, Option[BillingInformation]]
      }}.getOrElse(default)
    } else {
      default
    }

    val billingUpdate = fvApply(contactUpdates, (obi: Option[BillingInformation]) => {
      accountUpdateInfo.service.subscriptionId.map[FV[String, BillingData]] { s =>
        if (currentAccount.service.planId == accountUpdateInfo.service.planId) {
          Future.sync(Success(BillingData(obi, Some(s))))
        } else {
          val result = billingStore.changeSubscriptionPlan(s, accountUpdateInfo.service.planId)
          result.map(_.map(ns => BillingData(obi, Some(ns))))
        }
      }.getOrElse {
        Future.sync(Success(BillingData(obi, None))): FV[String, BillingData]
      }      
    })
    
    contactUpdates.then(billingUpdate)
  }

  private def combineAccountUpdates(info: AccountInformation, update: UpdateAccount): FV[String, AccountInformation] = {
    val email = validatedEmail(update.newEmail, info.id.email)
    val passwordHash = validatedPasswordHash(update.newPassword, info.id.passwordHash)
    val planId = update.newPlanId.getOrElse(info.service.planId)
    val contact = update.newContact.getOrElse(info.contact)

    email.map(ve => {
      (ve |@| passwordHash).apply((e, ph) => {
        info.copy(
          id = info.id.copy(email = e, passwordHash = ph),
          contact = contact,
          service = info.service.copy(planId = planId))
      })
    })
  }

  private def validatedPasswordHash(newPassword: Option[Password], originalHash: String): Validation[String, String] = {
    newPassword.map(p => if (p.confirmed)
      Success(PasswordHash.saltedHash(p.password))
    else
      Failure("New password and confirmation don't match."))
      .getOrElse(Success(originalHash))
  }

  private def validatedEmail(newEmail: Option[String], origEmail: String): FV[String, String] = {
    newEmail.map[FV[String, String]] { ne =>
      accountStore.getByEmail(ne).map(_.fold(_ => Success(ne),
        _ => Failure("An account associated with this email already exists.")))
    }.getOrElse(Future.sync(Success(origEmail)))
  }

  def getBillingInformation(auth: AccountAuthentication): FV[String, Option[BillingInformation]] = {
    getAuthenticatedAccount(auth).map(_.map(_.billing))
  }

  def updateBillingInformation(update: UpdateBilling): FV[String, BillingInformation] = {
    fvApply(getAuthenticatedAccount(update.authentication), (a: Account) => {
      billingStore.update(a.id.tokens.master, a.id.email, a.contact, update.billing)
    })
  }

  def removeBillingInformation(auth: AccountAuthentication): FV[String, Option[BillingInformation]] = {
    fvApply(getAuthenticatedAccount(auth), (a: Account) => {
      if (a.hasBillingInfo) {
        billingStore.removeByToken(a.id.tokens.master).map(_.map(Some(_)))
      } else {
        Future.sync(Success(None))
      }
    })
  }

  private def getAuthenticatedAccount(auth: AccountAuthentication): FV[String, Account] = {
    fvApply(
      fvApply(getAccountInfo(auth.email), authorizeAccountAccess(auth, _: AccountInformation)),
      combineWithBillingInfo(auth.email, _: AccountInformation))
  }

  def trustedGetAccount(email: String): FV[String, Account] = {
    fvApply(getAccountInfo(email), combineWithBillingInfo(email, _: AccountInformation))
  }

  private def getAccountInfo(email: String): FV[String, AccountInformation] = {
    accountStore.getByEmail(email)
  }

  private def authorizeAccountAccess(auth: AccountAuthentication, accountInfo: AccountInformation): FV[String, AccountInformation] = {
    if (PasswordHash.checkSaltedHash(auth.password, accountInfo.id.passwordHash)) {
      Future.sync(Success(accountInfo))
    } else {
      Future.sync(Failure("You must provide a valid email or account token and a valid password."))
    }
  }

  private def combineWithBillingInfo(email: String, accountInfo: AccountInformation): FV[String, Account] = {
    fvApply(getBillingInfo(email), (billing: Option[BillingInformation]) => {
      Future.sync(Success(
        ValueAccount(
          accountInfo.id,
          accountInfo.contact,
          accountInfo.service,
          billing)))
    })
  }

  private def combineWithBillingInfo(accountInfo: AccountInformation): FV[String, Account] = {
    val billing: FV[String, Option[BillingInformation]] = billingStore.getByEmail(accountInfo.id.email).map(v => Success(v.toOption))
    fvApply(billing, (billing: Option[BillingInformation]) => {
      Future.sync(Success(
        ValueAccount(
          accountInfo.id,
          accountInfo.contact,
          accountInfo.service,
          billing)))
    })
  }

  private def getBillingInfo(email: String): FV[String, Option[BillingInformation]] = {
    billingStore.getByEmail(email).map(v => Success(v.toOption))
  }

  def creditAccounting(): FV[String, CreditAccountingResults] = {
    val now = new DateTime(DateTimeZone.UTC).toDateMidnight.toDateTime(DateTimeZone.UTC)
    getAll().flatMap { accs =>
      val assessment = accs.foldLeft(Future.sync(new CreditAccountingResults))(creditAccounting(now))
      assessment.map(Success(_))
    }
  }

  private def creditAccounting(now: DateTime)(ass: Future[CreditAccountingResults], acc: Validation[String, Account]): Future[CreditAccountingResults] = {
    acc.fold(_ => ass, a => accountCreditAccounting(now)(ass, a))
  }

  private def accountCreditAccounting(now: DateTime)(ass: Future[CreditAccountingResults], acc: Account): Future[CreditAccountingResults] = {
    val daysBetween = Days.daysBetween(acc.service.lastCreditAssessment, now).getDays
    if (daysBetween <= 0) {
      ass
    } else {
      val creditAssessed = adjustCredit(acc, now, daysBetween)
      val subscriptionAdjusted = adjustSubscription(creditAssessed, now)
      subscriptionAdjusted.map(_.map(s => {
        if (acc != s) {
          trustedUpdateAccount(s).map(Success(_))
        } else {
          Future.sync(Success(s))
        }
      })).then(ass)
    }
  }

  private def adjustCredit(acc: Account, now: DateTime, daysBetween: Int): Account = {
    val service = acc.service
    if (service.credit <= 0) {
      val newService = service.copy(lastCreditAssessment = now, credit = 0)
      ValueAccount(
        acc.id,
        acc.contact,
        newService,
        acc.billing)
    } else {
      val dayRate = dailyRate(service)
      val adjustedCredit = service.credit - (daysBetween * dayRate)
      val newCredit = math.max(0, adjustedCredit)

      val newService = service.copy(lastCreditAssessment = now, credit = newCredit)

      ValueAccount(
        acc.id,
        acc.contact,
        newService,
        acc.billing)
    }
  }

  private val daysPerYear = 365
  private val monthsPerYear = 12
  private val averageDaysPerMonth = 1.0 * 365 / 12

  private def dailyRate(service: ServiceInformation): Int = {
    val plan = billingStore.findPlan(service.planId)
    val monthlyRate: Float = plan.map(p => p.getPrice().floatValue).getOrElse(0)
    val dailyRate = monthlyRate / averageDaysPerMonth
    (dailyRate * 100).toInt
  }

  private def adjustSubscription(acc: Account, now: DateTime): FV[String, Account] = {
    if (acc.service.status == AccountStatus.ACTIVE) {
      if (acc.service.credit > 0) {
        if (acc.service.subscriptionId.isDefined) {
          stopSubscription(acc)
        } else {
          Future.sync(Success(acc))
        }
      } else {
        if (acc.billing.isDefined) {
          startSubscription(acc)
        } else {
          disableAccount(acc, now)
        }
      }
    } else {
      Future.sync(Success(acc))
    }
  }

  private def stopSubscription(acc: Account): FV[String, Account] = {
    val newAccount: Account = ValueAccount(
      acc.id,
      acc.contact,
      acc.service.copy(subscriptionId = None),
      acc.billing)

    acc.service.subscriptionId.map(id => {
      billingStore.stopSubscriptionBySubscriptionId(id).map(_.map(_ => newAccount))
    }).getOrElse(Future.sync(Success(newAccount)))
  }

  private def startSubscription(acc: Account): FV[String, Account] = {
    val result: Option[FV[String, Account]] = acc.billing.map(_ => {
      billingStore.startSubscription(acc.id.tokens.master, acc.service.planId).map {
        case Success(s) => {
          Success(ValueAccount(
            acc.id,
            acc.contact,
            acc.service.copy(subscriptionId = Some(s)),
            acc.billing))
        }
        case Failure(e) => Failure(e)
      }
    })

    result.getOrElse(Future.sync(Failure("No billing information to start subscription from.")))
  }

  private def disableAccount(acc: Account, now: DateTime): FV[String, Account] = {
    Future.sync(Success(ValueAccount(
      acc.id,
      acc.contact,
      acc.service.copy(status = AccountStatus.DISABLED, lastAccountStatusChange = Some(now)),
      acc.billing)))
  }

  def getAll(): Future[List[Validation[String, Account]]] = {
    accountStore.getAll().flatMap(l => {
      Future.apply(l.toArray[AccountInformation].map(combineWithBillingInfo): _*)
    })
  }

  def usageAccounting(): FV[String, UsageAccountingResults] = {
    val now = new DateTime(DateTimeZone.UTC)
    getAll().flatMap { accs =>
      val assessment = accs.foldLeft(Future.sync(new UsageAccountingResults))(usageAccounting(now))
      assessment.map(Success(_))
    }
  }

  private val usageProcessingLag = 5
  private val usageMinimumCheckDelay = 5

  private class Plans(plans: List[(String, Long)]) {

    def findPlanMax(planId: String): Option[Long] = {
      plans.filter(_._1.equals(planId)) match {
        case x :: Nil => Some(x._2)
        case _ => None
      }
    }

    def upgrade(amount: Long): Option[String] = {
      plans.filter(_._2 > amount) match {
        case x :: xs => Some(x._1)
        case Nil => None
      }
    }
  }

  private val plans = new Plans(
    ("starter", 100000l) ::
      ("bronze", 1000000l) ::
      ("silver", 3000000l) ::
      ("gold", 12000000l) :: Nil)

  private def usageAccounting(now: DateTime)(ass: Future[UsageAccountingResults], acc: Validation[String, Account]): Future[UsageAccountingResults] = {
    acc.fold(_ => ass, a => accountUsageAccounting(now)(ass, a))
  }

  private def accountUsageAccounting(now: DateTime)(ass: Future[UsageAccountingResults], acc: Account): Future[UsageAccountingResults] = {
    if (acc.service.status == AccountStatus.ACTIVE) {
      val startDate = billingStartDate(now, acc.service.billingDay)
      val updatedUsage = getUsage(usageClient.apiCalls(acc.id.tokens.tracking, startDate.toDateMidnight, now.toDateMidnight().plusDays(1)))
      val newService = acc.service.copy(usage = acc.service.usage + updatedUsage, lastUsageAssessment = now)
      val usageUpdated = ValueAccount(
        id = acc.id,
        contact = acc.contact,
        service = newService,
        billing = acc.billing)

      val alreadySentWarning = acc.service.lastUsageWarning.map(last =>
        inThisBillingCycle(last, acc)).getOrElse(false)

      val warningUpdated = if (alreadySentWarning) {
        usageUpdated
      } else {
        warnAboutUsage(now, usageUpdated)
      }

      val upgradeUpdated = automatticallyUpgradePlan(now, warningUpdated)
      trustedUpdateAccount(upgradeUpdated).then(ass)
    } else {
      ass
    }
  }

  private def trustedUpdateAccount(acc: Account): Future[Unit] = {
    val accUpdate = accountStore.update(acc.asAccountInformation)
    val billingUpdate = acc.billing.map(billingStore.update(acc.id.tokens.master, acc.id.email, acc.contact, _))
    billingUpdate.map(accUpdate.then(_).toUnit).getOrElse(accUpdate.toUnit)
  }

  def trustedCreateAccount(acc: Account): FV[String, Account] = {
    // create billing information if required
    // if billing information and subscription required create subscription
    // update acc if with subscription id if necessary
    // create account information
    // get full account and return
    val billingCreate = acc.billing.map(billingStore.create(acc.id.tokens.master, acc.id.email, acc.contact, _))

    val subscriptionCreate: FV[String, Option[String]] = billingCreate.map {
      fvApply(_, (fbi: BillingInformation) => {
        val r1: Option[FV[String, String]] = acc.service.subscriptionId
          .map(_ => billingStore.startSubscription(acc.id.tokens.master, acc.service.planId))
        val r2: Option[FV[String, Option[String]]] = r1.map(_.map(_.map(Some(_))))
        val r3: FV[String, Option[String]] = r2.getOrElse(Future.sync(Success(None)))
        r3
      })
    }.getOrElse(Future.sync(Success(None)))

    val accCreate = fvApply(subscriptionCreate, (subsId: Option[String]) => {
      val accInfo = AccountInformation(
        acc.id,
        acc.contact,
        acc.service.copy(subscriptionId = subsId))
      accountStore.create(accInfo)
    })

    fvApply(accCreate, (_: AccountInformation) => trustedGetAccount(acc.id.email))
  }

  private val warningMinDaysForTrending = 7
  private val warningUsageThreshold = 0.90

  private def warnAboutUsage(now: DateTime, acc: Account): Account = {
    val startDate = billingStartDate(now, acc.service.billingDay)
    val finishDate = billingFinishDate(now, acc.service.billingDay)
    val total = Days.daysBetween(startDate, finishDate).getDays
    val used = Days.daysBetween(startDate, now).getDays

    if (used >= warningMinDaysForTrending) {
      val percentage = used.toDouble / total
      val max = plans.findPlanMax(acc.service.planId)
      max.flatMap(m => {
        val proratedLimit = m * percentage
        val warningLevel = proratedLimit * warningUsageThreshold
        if (acc.service.usage > warningLevel) {
          val updatedAccount = ValueAccount(
            id = acc.id,
            contact = acc.contact,
            service = acc.service.copy(lastUsageWarning = Some(now)),
            billing = acc.billing)
          sendUsageWarning(updatedAccount)
          Some(updatedAccount)
        } else {
          None
        }
      }).getOrElse(acc)
    } else {
      acc
    }
  }

  private def inThisBillingCycle(dateTime: DateTime, acc: Account): Boolean = {
    val startDate = billingStartDate(dateTime, acc.service.billingDay)
    (!dateTime.isBefore(startDate)) && dateTime.isBefore(startDate.plusMonths(1))
  }

  private def automatticallyUpgradePlan(now: DateTime, acc: Account): Account = {
    val max = plans.findPlanMax(acc.service.planId)

    max.map {
      case max if acc.service.usage > max => {
        plans.upgrade(acc.service.usage) match {
          case Some(newPlanId) => upgradePlan(newPlanId, acc)
          case None => noUpgradePath(now, acc)
        }
      }
      case _ => {
        acc
      }
    }.getOrElse(acc)
  }

  private def upgradePlan(newPlanId: String, acc: Account): Account = {
    sendUpgradeConfirmation(newPlanId, acc)
    ValueAccount(
      acc.id,
      acc.contact,
      acc.service.copy(planId = newPlanId),
      acc.billing)
  }

  private def noUpgradePath(now: DateTime, acc: Account): Account = {
    val warnedAlreadyThisMonth = acc.service.lastNoUpgradeWarning.map(last => {
      inThisBillingCycle(last, acc)
    }).getOrElse(false)

    if (!warnedAlreadyThisMonth) {
      sendNoUpgradePathWarning(acc)
      ValueAccount(
        acc.id,
        acc.contact,
        acc.service.copy(lastNoUpgradeWarning = Some(now)),
        acc.billing)
    } else {
      acc
    }
  }

  private def sendUsageWarning(acc: Account) {
    notifications.send(new UpgradeWarning(acc.id.email, Array("support@reportgrid.com"), Array(), Array("nick@reportgrid.com")))
  }

  private def sendUpgradeConfirmation(newPlanId: String, acc: Account) {
    notifications.send(new UpgradeConfirmation(acc.id.email, Array("support@reportgrid.com"), Array(), Array("nick@reportgrid.com")))
  }

  private def sendNoUpgradePathWarning(acc: Account) {
    notifications.send(new NoUpgradeWarning(acc.id.email, Array("support@reportgrid.com"), Array(), Array("nick@reportgrid.com")))
  }

  private def getUsage(f: Future[Long]): Long = {
    val nf = f.orElse(0)
    while (!nf.isDone) {}
    nf.value.get
  }

}

trait AccountInformationStore {
  type FV[E, A] = Future[Validation[E, A]]

  def create(info: AccountInformation): FV[String, AccountInformation]

  def getByEmail(email: String): FV[String, AccountInformation]
  def getByToken(token: String): FV[String, AccountInformation]

  def getAll(): Future[List[AccountInformation]]

  def update(info: AccountInformation): FV[String, AccountInformation]

  def disableByEmail(email: String): FV[String, AccountInformation]
  def disableByToken(token: String): FV[String, AccountInformation]

  def removeByEmail(email: String): FV[String, AccountInformation]
  def removeByToken(token: String): FV[String, AccountInformation]
}

class MongoAccountInformationStore(database: Database, collection: String) extends AccountInformationStore {

  import FutureValidationHelpers._

  def create(info: AccountInformation): FV[String, AccountInformation] = {
    val query = insert((info.serialize(AccountInformation.UnsafeAccountInfoDecomposer)) --> classOf[JObject]).into(collection)
    val futureResult = database(query)

    futureResult.map[Validation[String, AccountInformation]] { _ =>
      Success(info)
    }.orElse { ot: Option[Throwable] =>
      ot.map { t: Throwable =>
        val message = t.getMessage
        if (message.startsWith("E11000")) {
          Failure("An account associated with this email already exists.")
        } else {
          Failure(message)
        }
      }.getOrElse {
        Failure("Error initializing account.")
      }
    }
  }

  def getByEmail(email: String): FV[String, AccountInformation] = {
    val query = select().from(collection).where(("id.email" === email))
    singleAccountQuery(query)
  }

  def getByToken(token: String): FV[String, AccountInformation] = {
    val query = select().from(collection).where(("id.tokens.master" === token))
    singleAccountQuery(query)
  }

  def getAll(): Future[List[AccountInformation]] = {
    val query = select().from(collection)
    multipleAccountQuery(query)
  }

  private def singleAccountQuery(q: MongoSelectQuery): Future[Validation[String, AccountInformation]] = {
    val result = database(q)
    result.map { result =>
      val l = result.toList
      l.size match {
        case 1 => {
          Success(l.head.deserialize[AccountInformation])
        }
        case 0 => Failure("Account not found.")
        case _ => Failure("More than one account found.")
      }
    }
  }

  private def multipleAccountQuery(q: MongoSelectQuery): Future[List[AccountInformation]] = {
    val result = database(q)
    result.map { result =>
      result.toList.map(_.deserialize[AccountInformation])
    }
  }

  def update(newInfo: AccountInformation): FV[String, AccountInformation] = {
    fvApply(getByToken(newInfo.id.tokens.master), (current: AccountInformation) => {
      val updatedAccountInfo = validateAccountUpdates(current, newInfo)
      updatedAccountInfo match {
        case Success(ai) => internalUpdate(ai)
        case Failure(e) => Future.sync(Failure(e))
      }
    })
  }

  private def validateAccountUpdates(current: AccountInformation, newInfo: AccountInformation): Validation[String, AccountInformation] = {
    // Is it fair to assume that all the validation has been done so far???
    Success(newInfo)
  }

  private def internalUpdate(info: AccountInformation): FV[String, AccountInformation] = {
    val jObject = info.serialize(AccountInformation.UnsafeAccountInfoDecomposer).asInstanceOf[JObject]
    val query = MongoQueryBuilder.update(collection).set(jObject).where("id.tokens.master" === info.id.tokens.master)
    val result = database(query)
    result.flatMap { result =>
      getByToken(info.id.tokens.master)
    }.orElse(Failure("Error updating account information."))
  }

  def disableByEmail(email: String): FV[String, AccountInformation] = {
    fvApply(getByEmail(email), disable)
  }
  def disableByToken(token: String): FV[String, AccountInformation] = {

    fvApply(getByToken(token), disable)
  }

  private def disable(acc: AccountInformation): FV[String, AccountInformation] = {
    val updatedAccountInfo = acc.copy(service = acc.service.copy(status = AccountStatus.DISABLED))
    internalUpdate(updatedAccountInfo)
  }

  def removeByEmail(email: String): FV[String, AccountInformation] = sys.error("accountStore.removeByEmail Not yet implemented.")
  def removeByToken(token: String): FV[String, AccountInformation] = sys.error("accountStore.removeByToken Not yet implemented.")

}

trait BillingInformationStore {
  type FV[E, A] = Future[Validation[E, A]]

  def create(token: String, email: String, contact: ContactInformation, info: BillingInformation): FV[String, BillingInformation]

  def getByEmail(email: String): FV[String, BillingInformation]
  def getByToken(token: String): FV[String, BillingInformation]

  def update(token: String, email: String, contact: ContactInformation): FV[String, BillingInformation]
  def update(token: String, email: String, contact: ContactInformation, info: BillingInformation): FV[String, BillingInformation]

  def removeByEmail(email: String): FV[String, BillingInformation]
  def removeByToken(token: String): FV[String, BillingInformation]

  def startSubscription(token: String, planId: String): FV[String, String]

  def changeSubscriptionPlan(subscriptionId: String, planId: String): FV[String, String]

  def stopSubscriptionBySubscriptionId(subscriptionId: String): FV[String, String]

  def findPlans(): List[Plan]
  def findPlan(planId: String): Option[Plan]
}

case class SubscriptionData(
  id: String,
  billingDay: Int)

case class CreateAccount(
  email: String,
  password: String,
  confirmPassword: Option[String],
  planId: String,
  planCreditOption: Option[String],
  contact: ContactInformation,
  billing: Option[BillingInformation]) {

  def accountAuthentication = AccountAuthentication(email, password)
}

trait CreateAccountSerialization {

  implicit val CreateAccountDecomposer: Decomposer[CreateAccount] = new Decomposer[CreateAccount] {
    override def decompose(account: CreateAccount): JValue = JObject(
      List(
        JField("email", account.email.serialize),
        JField("planId", account.planId.serialize),
        JField("planCreditOption", account.planCreditOption.serialize),
        JField("contact", account.contact.serialize),
        JField("billing", account.billing.serialize)).filter(fieldHasValue))
  }

  implicit val CreateAccountExtractor: Extractor[CreateAccount] = new Extractor[CreateAccount] with ValidatedExtraction[CreateAccount] {
    override def validated(obj: JValue): Validation[Error, CreateAccount] =
      ((obj \ "email").validated[String] |@|
        (obj \ "password").validated[String] |@|
        (obj \ "confirmPassword").validated[Option[String]] |@|
        (obj \ "planId").validated[String] |@|
        (obj \ "planCreditOption").validated[Option[String]] |@|
        (obj \ "contact").validated[ContactInformation] |@|
        (obj \ "billing").validated[Option[BillingInformation]]).apply(CreateAccount(_, _, _, _, _, _, _))
  }
}

object CreateAccount extends CreateAccountSerialization

// Used for get and close account operations
case class AccountAuthentication(
  email: String,
  password: String)

trait AccountActionSerialization {
  implicit val AccountActionDecomposer: Decomposer[AccountAuthentication] = new Decomposer[AccountAuthentication] {
    override def decompose(action: AccountAuthentication): JValue = JObject(
      List(
        JField("email", action.email.serialize),
        JField("password", action.password.serialize)).filter(fieldHasValue))
  }

  implicit val AccountActionExtractor: Extractor[AccountAuthentication] = new Extractor[AccountAuthentication] with ValidatedExtraction[AccountAuthentication] {
    override def validated(obj: JValue): Validation[Error, AccountAuthentication] = (
      (obj \ "email").validated[String] |@|
      (obj \ "password").validated[String]).apply(AccountAuthentication(_, _))
  }
}

object AccountAuthentication extends AccountActionSerialization

case class Password(
  password: String,
  confirmPassword: String) {

  def confirmed: Boolean = password == confirmPassword
}

trait PasswordSerialization {
  implicit val PasswordDecomposer: Decomposer[Password] = new Decomposer[Password] {
    override def decompose(password: Password): JValue = JObject(
      List(
        JField("password", password.password.serialize),
        JField("confirmPassword", password.confirmPassword.serialize)).filter(fieldHasValue))
  }

  implicit val PasswordExtractor: Extractor[Password] = new Extractor[Password] with ValidatedExtraction[Password] {
    override def validated(obj: JValue): Validation[Error, Password] = (
      (obj \ "password").validated[String] |@|
      (obj \ "confirmPassword").validated[String]).apply(Password(_, _))
  }
}

object Password extends PasswordSerialization

case class UpdateAccount(
  auth: AccountAuthentication,
  newEmail: Option[String],
  newPassword: Option[Password],
  newPlanId: Option[String],
  newContact: Option[ContactInformation])

trait UpdateAccountSerialization {

  implicit val UpdateAccountDecomposer: Decomposer[UpdateAccount] = new Decomposer[UpdateAccount] {
    override def decompose(update: UpdateAccount): JValue = JObject(
      List(
        JField("authentication", update.auth.serialize),
        JField("newEmail", update.newEmail.serialize),
        JField("newPassword", update.newPassword.serialize),
        JField("newPlanId", update.newPlanId.serialize),
        JField("newContact", update.newContact.serialize)).filter(fieldHasValue))
  }

  implicit val UpdateAccountExtractor: Extractor[UpdateAccount] = new Extractor[UpdateAccount] with ValidatedExtraction[UpdateAccount] {
    override def validated(obj: JValue): Validation[Error, UpdateAccount] = (
      (obj \ "authentication").validated[AccountAuthentication] |@|
      (obj \ "newEmail").validated[Option[String]] |@|
      (obj \ "newPassword").validated[Option[Password]] |@|
      (obj \ "newPlanId").validated[Option[String]] |@|
      (obj \ "newContact").validated[Option[ContactInformation]]).apply(UpdateAccount(_, _, _, _, _))
  }
}

object UpdateAccount extends UpdateAccountSerialization

case class UpdateBilling(
  authentication: AccountAuthentication,
  billing: BillingInformation) {
}

trait UpdateBillingSerialization {

  val UnsafeUpdateBillingDecomposer: Decomposer[UpdateBilling] = new Decomposer[UpdateBilling] {
    override def decompose(update: UpdateBilling): JValue = JObject(
      List(
        JField("authentication", update.authentication.serialize),
        JField("billing", update.billing.serialize(BillingInformation.UnsafeBillingInfoDecomposer))).filter(fieldHasValue))
  }

  implicit val UpdateBillingDecomposer: Decomposer[UpdateBilling] = new Decomposer[UpdateBilling] {
    override def decompose(update: UpdateBilling): JValue = JObject(
      List(
        JField("authentication", update.authentication.serialize),
        JField("billing", update.billing.serialize)).filter(fieldHasValue))
  }

  implicit val UpdateBillingExtractor: Extractor[UpdateBilling] = new Extractor[UpdateBilling] with ValidatedExtraction[UpdateBilling] {
    override def validated(obj: JValue): Validation[Error, UpdateBilling] = (
      (obj \ "authentication").validated[AccountAuthentication] |@|
      (obj \ "billing").validated[BillingInformation]).apply(UpdateBilling(_, _))
  }

}

object UpdateBilling extends UpdateBillingSerialization

case class BillingData(
  info: Option[BillingInformation],
  subscriptionId: Option[String])

class UsageAccountingResults {
}

class CreditAccountingResults {
}

object TestAccounts {
  val testAccount = new ValueAccount(
    AccountId("john@doe.com", AccountTokens("master", "tracking", "prod", Some("dev")), "hash"),
    ContactInformation("john", "doe", "j co", "title", "j.co", "303-494-1893",
      Address("street", "city", "state", "60607")),
    ServiceInformation("starter",
      new DateTime(DateTimeZone.UTC), 0,
      new DateTime(DateTimeZone.UTC).toDateMidnight().toDateTime(DateTimeZone.UTC), 0,
      new DateTime(DateTimeZone.UTC).toDateMidnight().toDateTime(DateTimeZone.UTC), None, None,
      AccountStatus.ACTIVE, None, 31, None),
    None)
}
