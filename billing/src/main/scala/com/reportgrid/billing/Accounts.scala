package com.reportgrid.billing

import blueeyes.persistence.mongo._

import scala.collection.JavaConverters._

import scalaz._
import scalaz.Scalaz._

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.DateMidnight
import org.joda.time.Days

import com.braintreegateway._
import net.lag.configgy._

import blueeyes.concurrent.Future
import blueeyes.json.xschema._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.Extractor._
import Extractor.Error
import blueeyes.json.xschema.DefaultSerialization._
import com.reportgrid.billing.braintree.BraintreeService

import blueeyes.json.xschema.Extractor._

import SerializationHelpers._

class Accounts(config: ConfigMap, tokens: TokenGenerator, billingService: BraintreeService, database: Database, accountsCollection: String) {

  type FV[E, A] = Future[Validation[E, A]]

  def credits = config.getConfigMap("credits")

  def create(create: CreateAccount): FV[String, Account] = {
    val errs = validateCreate(create)
    errs match {
      case Some(s) => Future.sync(Failure(s))
      case _ => {
        val existingAccount = findByEmail(create.email)

        val credit = create.planCreditOption.flatMap { creditProgram =>
          credits.map { c =>
            val r = c.getInt(creditProgram, 0)
            r
          }
        }.getOrElse(0)

        existingAccount.flatMap {
          case Success(a) => Future.sync(Failure("An account associated with this email already exists."))
          case Failure(_) => {
            val token = newToken(toPath(create.email))

            token.flatMap[Validation[String, Account]] {
              case Success(t) => {
                val billing = establishBilling(create, t, credit > 0)

                billing match {
                  case Success(ob) => {
                    val tracking = createTrackingAccount(create, ob, t, credit)
                    tracking.map {
                      case Success(ta) => {
                        Success(buildAccount(ob, ta))
                      }
                      case Failure(e) => {
                        undoBilling(t)
                        undoNewToken(t)
                        Failure(e)
                      }
                    }
                  }
                  case Failure(e) => {
                    undoNewToken(t)
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
  }

  private def toPath(email: String): String = {
    val parts = email.split("[@.]")
    val sanitizedParts = parts.map(sanitize)
    "/" + sanitizedParts.reduceLeft((a, b) => b + "_" + a)
  }
  
  private def sanitize(s: String): String = {
    s.replaceAll("\\W", "_")
  }
  
  private def validateCreate(create: CreateAccount): Option[String] = {
    validateOther(create).orElse(validateBilling(create.contact.address.postalCode, create.billing))
  }

  private def validateOther(c: CreateAccount): Option[String] = {
    if (!validEmailAddress(c.email)) {
      Some("Invalid email address: " + c.email)
    } else if (c.password.length == 0) {
      Some("Password may not be zero length.")
    } else if (!planAvailable(c.planId)) {
      Some("The selected plan (" + c.planId + ") is not available.")
    } else {
      None
    }
  }

  private def validateBilling(postalCode: String, ob: Option[BillingInformation]): Option[String] = {
    ob match {
      case Some(b) if b.cardholder.trim().size == 0 => Some("Cardholder name required.")
      case Some(b) if postalCode.trim().size == 0 => Some("Postal code required.")
      case _ => None
    }
  }

  private val emailPattern = "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,4}".r
  private def validEmailAddress(e: String): Boolean = {
    emailPattern.pattern.matcher(e).matches()
  }

  private def planAvailable(p: String): Boolean = {
    val plans = billingService.findPlans()
    plans.any(plan => plan.getId() == p)
  }

  def updateAccount(a: Account): FV[String, Account] = Future.sync(Failure("Not yet implemented"))

  def cancelWithEmail(e: String, p: String): FV[String, Account] = Future.sync(Failure("Not yet implemented"))
  def cancelWithToken(t: String, p: String): FV[String, Account] = Future.sync(Failure("Not yet implemented"))

  def findByToken(t: String): FV[String, Account] = {
    val query = select().from(accountsCollection).where(("id.token" === t))
    findByQuery(query)
  }

  def findByEmail(e: String, forceNotFound: Boolean = false): FV[String, Account] = {
    if(forceNotFound) Future.sync(Failure("Account not found.")) else {
      val query = select().from(accountsCollection).where(("id.email" === e))
      findByQuery(query)
    }
  }

  private def findByQuery(q: MongoSelectQuery): FV[String, Account] = {
    val tracking = singleTrackingAccountQuery(q)
    tracking.map { vt =>
      {
        vt.flatMap { t =>
          val billing = getBilling(t)
          billing.map(b => buildAccount(t, b))
        }
      }
    }
  }

  def findAll(): Future[List[Validation[String, Account]]] = {
    val tracking = getAllTracking
    tracking.map { t =>
      t.map { t =>
        {
          val billing = getBilling(t)
          billing.map(b => buildAccount(t, b))
        }
      }
    }
  }

  def findPlans(): FV[String, List[Plan]] = Future.sync(Failure("Not yet implemented"))

  def audit(): FV[String, AuditResults] = Future.sync(Failure("Not yet implemented"))

  def assessment(): FV[String, AssessmentResults] = {
    val now = new DateTime(DateTimeZone.UTC).toDateMidnight.toDateTime
    findAll().map { accs =>
      val assessment = accs.foldLeft(new AssessmentResults)(accountValidationAssessment(now))
      Success(assessment)
    }
  }

  private def accountValidationAssessment(now: DateTime)(ass: AssessmentResults, acc: Validation[String, Account]): AssessmentResults = {
    acc.fold(_ => ass, a => accountAssessment(now)(ass, a))
  }

  private def accountAssessment(now: DateTime)(ass: AssessmentResults, acc: Account): AssessmentResults = {
    val daysBetween = Days.daysBetween(acc.service.lastCreditAssessment, now).getDays
    if (daysBetween <= 0) {
      ass
    } else {
      val creditAssessed = assessCredit(acc, now, daysBetween)
      val subscriptionAdjusted = adjustSubscription(creditAssessed, now)
      if (acc ne subscriptionAdjusted) {
        trustedUpdateAccount(acc.id.token, subscriptionAdjusted)
      } else {
      }
      ass
    }
  }

  private def assessCredit(acc: Account, now: DateTime, daysBetween: Int): Account = {
    val service = acc.service
    if (service.credit <= 0) {
      ValueAccount(
        acc.id,
        acc.contact,
        service.withNewCredit(now, 0),
        acc.billing)
    } else {
      val dayRate = dailyRate(service)
      val adjustedCredit = service.credit - (daysBetween * dayRate)
      val newCredit = math.max(0, adjustedCredit)

      ValueAccount(
        acc.id,
        acc.contact,
        service.withNewCredit(now, newCredit),
        acc.billing)
    }
  }

  private val daysPerYear = 365
  private val monthsPerYear = 12
  private val averageDaysPerMonth = 1.0 * 365 / 12

  private def dailyRate(service: ServiceInformation): Int = {
    val plan = billingService.findPlan(service.planId)
    val monthlyRate: Float = plan.map(p => p.getPrice().floatValue).getOrElse(0)
    val dailyRate = monthlyRate / averageDaysPerMonth
    (dailyRate * 100).toInt
  }

  private val graceDays = 7

  private def adjustSubscription(acc: Account, now: DateTime): Account = {
    if (acc.service.status == AccountStatus.GRACE_PERIOD) {
      disableIfGracePeriodExpired(acc, now)
    } else if (acc.service.status == AccountStatus.ACTIVE) {
      if (acc.service.credit > 0) {
        if (acc.service.subscriptionId.isDefined) {
          stopSubscription(acc)
        } else {
          acc
        }
      } else {
        if (acc.billing.isDefined) {
          startSubscription(acc)
        } else {
          startGracePeriod(acc, now)
        }
      }
    } else {
      acc
    }
  }

  private def disableIfGracePeriodExpired(acc: Account, now: DateTime): Account = {
    acc.service.gracePeriodExpires.map(exp => {
      if (now.isAfter(exp)) {
        ValueAccount(
          acc.id,
          acc.contact,
          acc.service.disableAccount,
          acc.billing)
      } else {
        acc
      }
    }).getOrElse(acc)
  }

  private def stopSubscription(acc: Account): Account = {
    billingService.stopSubscription(acc.service.subscriptionId)
    ValueAccount(
      acc.id,
      acc.contact,
      acc.service.withNewSubscription(None),
      acc.billing)
  }

  private def startSubscription(acc: Account): Account = {
    acc.billing.flatMap(b => {
      val cust = billingService.findCustomer(acc.id.token)
      cust.flatMap(c => {
        billingService.newSubscription(c, acc.service.planId) match {
          case Success(s) => {
            Some(ValueAccount(
              acc.id,
              acc.contact,
              acc.service.withNewSubscription(Some(s.getId())),
              acc.billing))
          }
          case _ => None
        }
      })
    }).getOrElse(acc)
  }

  private def startGracePeriod(acc: Account, now: DateTime): Account = {
    ValueAccount(
      acc.id,
      acc.contact,
      acc.service.activeGracePeriod(now.plusDays(graceDays)),
      acc.billing)
  }

  def trustedCreateAccount(acc: Account): Future[Unit] = {
    val tracking = acc.asTrackingAccount
    val billing = acc.asBillingAccount

    val b: Option[String] = billing.billing.flatMap(b => {
      val c = CreateAccount(
          acc.id.email,
          "",
          None,
          acc.service.planId,
          None,
          acc.contact,
          billing.billing
      )
      
      val newba = billingService.newCustomer(c, b, acc.id.token)
      val subs = billing.subscriptionId.flatMap { s =>
        val c = billingService.findCustomer(acc.id.token)
        c.map(c => {
          billingService.newSubscription(c, acc.service.planId)
        })
      }
      subs.flatMap {
        case Success(s) => Some(s.getId())
      }
    })

    val newTracking = TrackingAccount(
      tracking.id, tracking.contact, tracking.service.withNewSubscription(b))

    val jval: JValue = newTracking.serialize
    val q = insert(jval --> classOf[JObject]).into(accountsCollection)
    database(q)
  }

  private def trustedUpdateAccount(token: String, acc: Account): Future[Validation[String, Account]] = {
    val res = updateTracking(token, acc.asTrackingAccount)
    res.map(v => v.map(x => acc))
  }

  private def updateTracking(t: String, a: TrackingAccount): Future[Validation[String, Unit]] = {
    val jval: JValue = a.serialize
    val q = update(accountsCollection).set(jval --> classOf[JObject]).where("id.token" === t)
    val res: Future[Unit] = database(q)
    res.map { u =>
      Success(u)
    }
  }

  def newToken(path: String): Future[Validation[String, String]] = {
    tokens.newToken(path)
  }

  def undoNewToken(token: String): Unit = {
    tokens.deleteToken(token)
  }

  def establishBilling(create: CreateAccount, token: String, hasCredit: Boolean): Validation[String, Option[BillingAccount]] = {
    create.billing match {
      case Some(b) if hasCredit => {
        val cust = billingService.newCustomer(create, b, token)
        cust match {
          case Success(c) => {
            Success(Some(BillingAccount(Some(b), None)))
          }
          case Failure(e) => Failure(e)
        }
      }
      case Some(b) => {
        val cust = billingService.newCustomer(create, b, token)
        cust match {
          case Success(c) => {
            val subs = billingService.newSubscription(c, create.planId)
            subs match {
              case Success(s) => {
                Success(Some(BillingAccount(Some(b), Some(s.getId()))))
              }
              case Failure(e) => {
                val remove = billingService.removeCustomer(c.getId())
                remove match {
                  case Success(s) => Failure(e)
                  case Failure(e) => Failure(e + "(Unable to remove customer info.)")
                }
              }
            }
          }
          case Failure(e) => Failure(e)
        }
      }
      case None if hasCredit => {
        Success(None)
      }
      case None => {
        Failure("Unable to create account without account credit or billing information.")
      }
    }
  }

  def undoBilling(token: String): Unit = {
    println("Rolling back billing")
  }

  def createTrackingAccount(create: CreateAccount, billing: Option[BillingAccount], token: String, credit: Int): Future[Validation[String, TrackingAccount]] = {
    val created = new DateTime(DateTimeZone.UTC)
    val today = created.toDateMidnight().toDateTime()
    val aid = AccountId(token, create.email, PasswordHash.saltedHash(create.password))
    val srv = ServiceInformation(
      create.planId,
      created,
      credit,
      today,
      0,
      AccountStatus.ACTIVE,
      None,
      billing.flatMap(b => b.subscriptionId))

    val trackingAccount = TrackingAccount(aid, create.contact, srv)

    val query = insert((trackingAccount.serialize) --> classOf[JObject]).into(accountsCollection)
    val futureResult = database(query)
    
    futureResult.map[Validation[String, TrackingAccount]] { _ =>
      Success(trackingAccount)
    }.orElse { ot: Option[Throwable] => 
      ot.map{ t: Throwable =>
        val message = t.getMessage
        if(message.startsWith("E11000")) {
          Failure("An account associated with this email already exists.")
        } else {
          Failure(message)
        }
      }.getOrElse{
        Failure("Error initializing account.")
      }
    }
  }

  def buildAccount(billing: Option[BillingAccount], tracking: TrackingAccount): Account = {
    val service = tracking.service
    val newService = ServiceInformation(
      service.planId,
      service.accountCreated,
      service.credit,
      service.lastCreditAssessment,
      service.usage,
      service.status,
      service.gracePeriodExpires,
      billing.flatMap(b => b.subscriptionId))

    ValueAccount(
      tracking.id,
      tracking.contact,
      newService,
      billing.flatMap(b => b.billing))
  }

  def emailConfirmation(account: Account): Future[Validation[String, Account]] = {
    confirmationEmail(account)
    Future.sync(Success(account))
  }

  private def confirmationEmail(account: Account): Unit = {

  }

  private def getAllTracking(): Future[List[TrackingAccount]] = {
    val query = select().from(accountsCollection)
    multipleTrackingAccountQuery(query)
  }

  private def getBilling(tracking: TrackingAccount): Validation[String, BillingAccount] = {
    val cust = billingService.findCustomer(tracking.id.token)
    val b = cust.map(c => toBillingAccount(c)).
      getOrElse(BillingAccount(None, None))
    Success(b)
  }

  private def toBillingAccount(customer: Customer): BillingAccount = {
    val cards = customer.getCreditCards()
    val card = if (cards.size == 1) Some(cards.get(0)) else None
    val billingInfo = card.map { c =>
      {
        BillingInformation(
          c.getCardholderName(),
          c.getLast4(),
          c.getExpirationMonth().toInt,
          c.getExpirationYear().toInt,
          "")
      }
    }
    val subs = card.flatMap { c =>
      {
        val subs = c.getSubscriptions()
        if (subs.size == 1) Some(subs.get(0).getId()) else None
      }
    }
    BillingAccount(billingInfo, subs)
  }

  private def buildAccount(
    tracking: TrackingAccount,
    billing: BillingAccount): Account = {
    ValueAccount(
      tracking.id,
      tracking.contact,
      tracking.service,
      billing.billing)
  }

  private def singleTrackingAccountQuery(q: MongoSelectQuery): Future[Validation[String, TrackingAccount]] = {
    val result = database(q)
    result.map { result =>
      val l = result.toList
      l.size match {
        case 1 => {
          Success(l.head.deserialize[TrackingAccount])
        }
        case 0 => Failure("Account not found.")
        case _ => Failure("More than one account found.")
      }
    }
  }

  private def multipleTrackingAccountQuery(q: MongoSelectQuery): Future[List[TrackingAccount]] = {
    val result = database(q)
    result.map { result =>
      result.toList.map(_.deserialize[TrackingAccount])
    }
  }
}

case class TrackingAccount(
  id: AccountId,
  contact: ContactInformation,
  service: ServiceInformation)

trait TrackingAccountSerialization {

  implicit val TrackingAccountDecomposer: Decomposer[TrackingAccount] = new Decomposer[TrackingAccount] {
    
    implicit val idDecomposer = AccountId.UnsafeAccountIdDecomposer
    
    override def decompose(tracking: TrackingAccount): JValue = JObject(
      List(
        JField("id", tracking.id.serialize),
        JField("contact", tracking.contact.serialize),
        JField("service", tracking.service.serialize)).filter(fieldHasValue))
  }

  implicit val TrackingAccountExtractor: Extractor[TrackingAccount] = new Extractor[TrackingAccount] with ValidatedExtraction[TrackingAccount] {
    override def validated(obj: JValue): Validation[Error, TrackingAccount] = (
      (obj \ "id").validated[AccountId] |@|
      (obj \ "contact").validated[ContactInformation] |@|
      (obj \ "service").validated[ServiceInformation]).apply(TrackingAccount(_, _, _))
  }
}

object TrackingAccount extends TrackingAccountSerialization

case class BillingAccount(
  billing: Option[BillingInformation],
  subscriptionId: Option[String])

  
case class CreateAccount(
  email: String,
  password: String,
  confirmPassword: Option[String],
  planId: String,
  planCreditOption: Option[String],
  contact: ContactInformation,
  billing: Option[BillingInformation])

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

class AuditResults {
}

class AssessmentResults {
}

object TestAccounts {
  val testAccount = new ValueAccount(
    AccountId("token", "john@doe.com", "hash"),
    ContactInformation("john", "doe", "j co", "title", "j.co", "303-494-1893",
      Address("street", "city", "state", "60607")),
    ServiceInformation("starter", 
                       new DateTime(DateTimeZone.UTC), 0, 
                       new DateTime(DateTimeZone.UTC).toDateMidnight().toDateTime(), 0, 
                       AccountStatus.ACTIVE, None, None), 
    None)

  def testTrackingAccount: TrackingAccount = {
    val a = testAccount
    TrackingAccount(
      a.id,
      a.contact,
      a.service)
  }
}

