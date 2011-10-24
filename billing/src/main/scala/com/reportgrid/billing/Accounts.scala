package com.reportgrid.billing

import java.security.SecureRandom

import scala.collection.JavaConverters._

import scalaz._
import scalaz.Scalaz._

import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.codec.binary.Hex

import com.braintreegateway._
import net.lag.configgy._

import blueeyes.concurrent.Future
import blueeyes.persistence.mongo.Database
import blueeyes.persistence.mongo._
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

  def credits = config.getConfigMap("credits")

  type FV[E, A] = Future[Validation[E, A]]

  def create(signup: Signup, billingInfo: Option[BillingInformation]): FV[String, Account] = {
    val errs = validate(signup, billingInfo)
    errs match {
      case Some(s) => Future.sync(Failure(s))
      case _ => {
        val existingAccount = findByEmail(signup.email)

        val credit = signup.planCreditOption.flatMap { creditProgram =>
          credits.map { c =>
            val r = c.getInt(creditProgram, 0)
            r
          }
        }.getOrElse(0)

        existingAccount.flatMap {
          case Success(a) => Future.sync(Failure("An account associated with this email already exists."))
          case Failure(e) => {
            val token = newToken()

            token.flatMap[Validation[String, Account]] {
              case Success(t) => {
                val billing = establishBilling(signup, billingInfo, t, credit > 0)

                billing match {
                  case Success(ob) => {
                    val tracking = createTrackingAccount(signup, ob, t, credit)
                    tracking.map {
                      case Success(ta) => {
                        Success(buildAccount(signup, ob, ta))
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

  private def validate(s: Signup, b: Option[BillingInformation]): Option[String] = {
    validate(s).orElse(validate(b))
  }

  private def validate(s: Signup): Option[String] = {
    if (!validEmailAddress(s.email)) {
      Some("Invalid email address: " + s.email)
    } else if (s.password.length == 0) {
      Some("Password may not be zero length.")
    } else if (!planAvailable(s.planId)) {
      Some("The selected plan (" + s.planId + ") is not available.")
    } else {
      None
    }
  }

  private def validate(ob: Option[BillingInformation]): Option[String] = {
    ob match {
      case Some(b) if b.cardholder.trim().size == 0 => Some("Cardholder name required.")
      case Some(b) if b.postalCode.trim().size == 0 => Some("Postal code required.")
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

  def update(a: Account): FV[String, Account] = Future.sync(Failure("Not yet implemented"))

  def cancelWithEmail(e: String, p: String): FV[String, Account] = Future.sync(Failure("Not yet implemented"))
  def cancelWithToken(t: String, p: String): FV[String, Account] = Future.sync(Failure("Not yet implemented"))

  def findByToken(t: String): FV[String, Account] = {
    val query = select().from(accountsCollection).where(("accountToken" === t))
    findByQuery(query)
  }

  def findByEmail(e: String): FV[String, Account] = {
    val query = select().from(accountsCollection).where(("email" === e))
    findByQuery(query)
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

  def assessment(): FV[String, AssessmentResults] = Future.sync(Failure("Not yet implemented"))

  def newToken(): Future[Validation[String, String]] = {
    tokens.newToken()
  }

  def undoNewToken(token: String): Unit = {
    tokens.deleteToken(token)
  }

  def establishBilling(signup: Signup, billingInfo: Option[BillingInformation], token: String, hasCredit: Boolean): Validation[String, Option[BillingAccount]] = {
    billingInfo match {
      case Some(b) if hasCredit => {
        val cust = billingService.newUserAndCard(signup, b, token)
        cust match {
          case Success(c) => {
            Success(Some(BillingAccount(Some(b), None)))
          }
          case Failure(e) => Failure(e)
        }
      }
      case Some(b) => {
        val cust = billingService.newUserAndCard(signup, b, token)
        cust match {
          case Success(c) => {
            val subs = billingService.newSubscription(c, signup.planId)
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

  def createTrackingAccount(signup: Signup, billing: Option[BillingAccount], token: String, credit: Int): Future[Validation[String, TrackingAccount]] = {
    val trackingAccount = TrackingAccount(
      signup.email,
      signup.company,
      signup.website,
      signup.planId,
      PasswordHash.saltedHash(signup.password),
      billing.flatMap(b => b.subscriptionId),
      token,
      credit,
      0,
      AccountStatus.ACTIVE)

    val query = insert((trackingAccount.serialize) --> classOf[JObject]).into(accountsCollection)
    val futureResult = database(query)
    futureResult.map[Validation[String, TrackingAccount]](queryResult => Success(trackingAccount)).orElse(t => Failure("Error initializing account."))
  }

  def buildAccount(signup: Signup, billing: Option[BillingAccount], tracking: TrackingAccount): Account = {
    ValueAccount(
      tracking.email,
      tracking.company,
      tracking.website,
      tracking.planId,
      tracking.passwordHash,
      tracking.accountToken,
      tracking.accountCredit,
      tracking.accountUsage,
      tracking.accountStatus,
      billing.flatMap(b => b.subscriptionId),
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
    val cust = billingService.findCustomer(tracking.accountToken)
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
          "",
          c.getBillingAddress().getPostalCode())
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
      tracking.email,
      tracking.company,
      tracking.website,
      tracking.planId,
      tracking.passwordHash,
      tracking.accountToken,
      tracking.accountCredit,
      tracking.accountUsage,
      tracking.accountStatus,
      billing.subscriptionId,
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

object SerializationHelpers {
  def fieldHasValue(field: JField): Boolean = field.value match {
    case JNull => false
    case _ => true
  }
}

object AccountStatus extends Enumeration {
  type AccountStatus = Value
  val SETUP, ACTIVE, DISABLED, ERROR = Value

  implicit val AccountDecomposer: Decomposer[AccountStatus] = new Decomposer[AccountStatus] {
    override def decompose(status: AccountStatus): JValue = JString(status.toString())
  }

  implicit val AccountExtractor: Extractor[AccountStatus] = new Extractor[AccountStatus] {
    override def extract(jvalue: JValue): AccountStatus = {
      jvalue match {
        case JString(s) => {
          try {
            AccountStatus.withName(s)
          } catch {
            case ex => ERROR
          }
        }
        case _ => ERROR
      }
    }
  }

  // Extractor not required as all instances are created with this service
}

import AccountStatus._

trait Account {
  def email: String
  def company: Option[String]
  def website: Option[String]
  def planId: String
  def passwordHash: String
  def accountToken: String
  def accountCredit: Int
  def accountUsage: Long
  def accountStatus: AccountStatus
  def subscriptionId: Option[String]
  def billing: Option[BillingInformation]

  def hasSubscription: Boolean = subscriptionId.isDefined
  def hasBillingInfo: Boolean = billing.isDefined
}

case class AccountId(
  token: String,
  email: String,
  passwordHash: String)

trait AccountIdSerialization {
  
  implicit val AccountIdDecomposer: Decomposer[AccountId] = new Decomposer[AccountId] {
    override def decompose(id: AccountId): JValue = JObject(
      List(
        JField("token", id.token.serialize),
        JField("email", id.email.serialize)))
  }

  implicit val AccountIdExtractor: Extractor[AccountId] = new Extractor[AccountId] with ValidatedExtraction[AccountId] {
    override def validated(obj: JValue): Validation[Error, AccountId] = (
      (obj \ "token").validated[String] |@|
      (obj \ "email").validated[String] |@|
      (obj \ "passwordHash").validated[String]).apply(AccountId(_, _, _))
  }

}

object AccountId extends AccountIdSerialization

case class ContactInformation(
  firstName: Option[String],
  lastName: Option[String],
  company: Option[String],
  title: Option[String],
  phone: Option[String],
  website: Option[String],
  address: Address)
  
trait ContactInformationSerialization {

  implicit val ContactInformationDecomposer: Decomposer[ContactInformation] = new Decomposer[ContactInformation] {
    override def decompose(contact: ContactInformation): JValue = JObject(
      List(
        JField("firstName", contact.firstName.serialize),
        JField("lastName", contact.lastName.serialize),
        JField("company", contact.company.serialize),
        JField("title", contact.company.serialize),
        JField("phone", contact.company.serialize),
        JField("website", contact.website.serialize),
        JField("address", contact.address.serialize)).filter(fieldHasValue))
  }

  implicit val ContactInformationExtractor: Extractor[ContactInformation] = new Extractor[ContactInformation] with ValidatedExtraction[ContactInformation] {
    override def validated(obj: JValue): Validation[Error, ContactInformation] = (
      (obj \ "firstName").validated[Option[String]] |@|
      (obj \ "lastName").validated[Option[String]] |@|
      (obj \ "company").validated[Option[String]] |@|
      (obj \ "title").validated[Option[String]] |@|
      (obj \ "website").validated[Option[String]] |@|
      (obj \ "phone").validated[Option[String]] |@|
      (obj \ "address").validated[Address]).apply(ContactInformation(_, _, _, _, _, _, _))
  }

}

object ContactInformation extends ContactInformationSerialization

case class Address(
  street: Option[String],
  city: Option[String], //(region)
  state: Option[String], //(locality)
  postalCode: String)
  
trait AddressSerialization {
  
  implicit val AddressDecomposer: Decomposer[Address] = new Decomposer[Address] {
    override def decompose(address: Address): JValue = JObject(
      List(
        JField("street", address.street.serialize),
        JField("city", address.city.serialize),
        JField("state", address.state.serialize),
        JField("postalCode", address.postalCode.serialize)))
  }

  implicit val AddressExtractor: Extractor[Address] = new Extractor[Address] with ValidatedExtraction[Address] {
    override def validated(obj: JValue): Validation[Error, Address] = (
      (obj \ "street").validated[Option[String]] |@|
      (obj \ "city").validated[Option[String]] |@|
      (obj \ "state").validated[Option[String]] |@|
      (obj \ "postalCode").validated[String]).apply(Address(_, _, _, _))
  }

}

object Address extends AddressSerialization

case class ServiceInformation(
  planId: String,
  credit: Int,
  usage: Long,
  status: AccountStatus,
  subscriptionId: Option[String])

trait ServiceInformationSerialization {
  
  implicit val ServiceInformationDecomposer: Decomposer[ServiceInformation] = new Decomposer[ServiceInformation] {
    override def decompose(address: ServiceInformation): JValue = JObject(
      List(
        JField("planId", address.planId.serialize),
        JField("credit", address.credit.serialize),
        JField("usage", address.usage.serialize),
        JField("status", address.status.serialize),
        JField("subscriptionId", address.subscriptionId.serialize)))
  }

  implicit val ServiceInformationExtractor: Extractor[ServiceInformation] = new Extractor[ServiceInformation] with ValidatedExtraction[ServiceInformation] {
    override def validated(obj: JValue): Validation[Error, ServiceInformation] = (
      (obj \ "planId").validated[String] |@|
      (obj \ "credit").validated[Int] |@|
      (obj \ "usage").validated[Long] |@|
      (obj \ "status").validated[AccountStatus] |@|
      (obj \ "subscriptionId").validated[Option[String]]).apply(ServiceInformation(_, _, _, _, _))
  }
}

object ServiceInformation extends ServiceInformationSerialization
  
case class NewValueAccount(
    id: AccountId,
    contact: ContactInformation,
    service: ServiceInformation,
    billing: Option[BillingInformation]
)

case class ValueAccount(
  val email: String,
  val company: Option[String],
  val website: Option[String],
  val planId: String,
  val passwordHash: String,
  val accountToken: String,
  val accountCredit: Int,
  val accountUsage: Long,
  val accountStatus: AccountStatus,
  val subscriptionId: Option[String],
  val billing: Option[BillingInformation]) extends Account

trait AccountSerialization {
  implicit val AccountDecomposer: Decomposer[Account] = new Decomposer[Account] {
    override def decompose(account: Account): JValue = JObject(
      List(
        JField("email", account.email.serialize),
        JField("company", account.company.serialize),
        JField("website", account.website.serialize),
        JField("planId", account.planId.serialize),
        JField("accountToken", account.accountToken.serialize),
        JField("accountCredit", account.accountCredit.serialize),
        JField("accountUsage", account.accountUsage.serialize),
        JField("accountStatus", account.accountStatus.serialize),
        JField("subscriptionId", account.subscriptionId.serialize),
        JField("billing", account.billing.serialize)).filter(fieldHasValue))
  }

  implicit val AccountExtractor: Extractor[Account] = new Extractor[Account] with ValidatedExtraction[Account] {
    override def validated(obj: JValue): Validation[Error, Account] = (
      (obj \ "email").validated[String] |@|
      (obj \ "company").validated[Option[String]] |@|
      (obj \ "website").validated[Option[String]] |@|
      (obj \ "planId").validated[String] |@|
      (obj \ "accountToken").validated[String] |@|
      (obj \ "accountCredit").validated[Int] |@|
      (obj \ "accountUsage").validated[Long] |@|
      (obj \ "accountStatus").validated[AccountStatus] |@|
      (obj \ "subscriptionId").validated[Option[String]] |@|
      (obj \ "billing").validated[Option[BillingInformation]]).apply(
        ValueAccount(_, _, _, _, (obj \ "passwordHash").validated[String] | "", _, _, _, _, _, _))
  }
}

object Account extends AccountSerialization

case class TrackingAccount(
  email: String,
  company: Option[String],
  website: Option[String],
  planId: String,
  passwordHash: String,
  subscriptionId: Option[String],
  accountToken: String,
  accountCredit: Int,
  accountUsage: Long,
  accountStatus: AccountStatus)

trait TrackingAccountSerialization {

  implicit val TrackingAccountDecomposer: Decomposer[TrackingAccount] = new Decomposer[TrackingAccount] {
    override def decompose(tracking: TrackingAccount): JValue = JObject(
      List(
        JField("email", tracking.email.serialize),
        JField("company", tracking.company.serialize),
        JField("website", tracking.website.serialize),
        JField("planId", tracking.planId.serialize),
        JField("passwordHash", tracking.passwordHash.serialize),
        JField("subscriptionId", tracking.accountToken.serialize),
        JField("accountToken", tracking.accountToken.serialize),
        JField("accountCredit", tracking.accountCredit.serialize),
        JField("accountUsage", tracking.accountUsage.serialize),
        JField("accountStatus", tracking.accountStatus.serialize)).filter(fieldHasValue))
  }

  implicit val TrackingAccountExtractor: Extractor[TrackingAccount] = new Extractor[TrackingAccount] with ValidatedExtraction[TrackingAccount] {
    override def validated(obj: JValue): Validation[Error, TrackingAccount] = (
      (obj \ "email").validated[String] |@|
      (obj \ "company").validated[Option[String]] |@|
      (obj \ "website").validated[Option[String]] |@|
      (obj \ "planId").validated[String] |@|
      (obj \ "passwordHash").validated[String] |@|
      (obj \ "subscriptionId").validated[Option[String]] |@|
      (obj \ "accountToken").validated[String] |@|
      (obj \ "accountCredit").validated[Int] |@|
      (obj \ "accountUsage").validated[Long] |@|
      (obj \ "accountStatus").validated[AccountStatus]).apply(TrackingAccount(_, _, _, _, _, _, _, _, _, _))
  }
}

object TrackingAccount extends TrackingAccountSerialization

case class BillingAccount(
  billing: Option[BillingInformation],
  subscriptionId: Option[String])

object PasswordHash {

  val saltBytes = 20

  def saltedHash(password: String): String = {
    val salt = generateSalt
    val hash = hashWithSalt(salt, password)
    salt + hash
  }

  def generateSalt(): String = {
    val random = new SecureRandom()
    val salt = random.generateSeed(saltBytes)
    Hex.encodeHexString(salt).toUpperCase()
  }

  def checkSaltedHash(password: String, saltedHash: String): Boolean = {
    val saltLength = saltBytes * 2
    val salt = saltedHash.substring(0, saltLength)
    val hash = saltedHash.substring(saltLength)
    val testHash = hashWithSalt(salt, password)
    hash == testHash
  }

  private def hashWithSalt(salt: String, password: String): String = {
    hashFunction(salt + password)
  }

  private def hashFunction(password: String): String = {
    DigestUtils.shaHex(password).toUpperCase()
  }
}

case class BillingInformation(
  cardholder: String,
  number: String,
  expMonth: Int,
  expYear: Int,
  cvv: String,
  postalCode: String) {

  def expDate = expMonth + "/" + expYear

  def safeNumber: String = {
    val show = 4
    val required = 16
    val size = number.length
    val fill = required - (if (size >= show) show else size)
    ("*" * fill) + (if (size >= show) number.substring(size - show) else number)
  }
}

trait BillingInfoSerialization {

  implicit val BillingInfoDecomposer: Decomposer[BillingInformation] = new Decomposer[BillingInformation] {
    override def decompose(billing: BillingInformation): JValue = JObject(
      List(
        JField("cardholder", billing.cardholder.serialize),
        JField("number", billing.safeNumber.serialize),
        JField("expMonth", billing.expMonth.serialize),
        JField("expYear", billing.expYear.serialize),
        JField("postalCode", billing.postalCode.serialize)))
  }

  implicit val BillingInfoExtractor: Extractor[BillingInformation] = new Extractor[BillingInformation] with ValidatedExtraction[BillingInformation] {
    override def validated(obj: JValue): Validation[Error, BillingInformation] = (
      (obj \ "cardholder").validated[String] |@|
      (obj \ "number").validated[String] |@|
      (obj \ "expMonth").validated[Int] |@|
      (obj \ "expYear").validated[Int] |@|
      (obj \ "postalCode").validated[String]).apply(
        BillingInformation(_, _, _, _, (obj \ "cvv").validated[String] | "", _))
  }
}

object BillingInformation extends BillingInfoSerialization

case class Signup(
  email: String,
  company: Option[String],
  website: Option[String],
  planId: String,
  planCreditOption: Option[String],
  password: String)

class AuditResults {
}

class AssessmentResults {
}

trait Mailer {
  def send(): Future[Validation[String, Unit]]
}

class SendMailer extends Mailer {
  def send(): Future[Validation[String, Unit]] = Future.sync(Failure("Not yet implemented"))
}

class NullMailer extends Mailer {
  def send(): Future[Validation[String, Unit]] = Future.sync(Success(()))
}

object TestAccounts {
  val testAccount = new ValueAccount(
    "john@doe.com",
    Some("jdco"),
    Some("jd.co"),
    "starter",
    "hash",
    "token",
    0,
    0,
    AccountStatus.SETUP,
    None,
    None)

  def testTrackingAccount: TrackingAccount = {
    val a = testAccount
    TrackingAccount(
      a.email,
      a.company,
      a.website,
      a.planId,
      a.passwordHash,
      None,
      a.accountToken,
      a.accountCredit,
      a.accountUsage,
      a.accountStatus)
  }
}
