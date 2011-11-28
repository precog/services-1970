package com.reportgrid.billing

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import scalaz._
import scalaz.Scalaz._

import scala.collection.JavaConverters._

import blueeyes.json.xschema._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

object SerializationHelpers {
  def fieldHasValue(field: JField): Boolean = field.value match {
    case JNull => false
    case _ => true
  }
}

object AccountStatus extends Enumeration {
  type AccountStatus = Value
  val ACTIVE, DISABLED, ERROR = Value

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
import SerializationHelpers._

trait Account {
  def id: AccountId
  def contact: ContactInformation
  def service: ServiceInformation
  def billing: Option[BillingInformation]

  def hasSubscription: Boolean = service.subscriptionId.isDefined
  def hasBillingInfo: Boolean = billing.isDefined

  def asAccountInformation = {
    AccountInformation(id, contact, service)
  }  
}

case class AccountTokens(
    master: String,
    tracking: String,
    production: String,
    development: Option[String]) 
    
trait AccountTokensSerialization {

  val UnsafeAccountTokensDecomposer: Decomposer[AccountTokens] = new Decomposer[AccountTokens] {
  override def decompose(tokens: AccountTokens): JValue = JObject(
      List(
        JField("master", tokens.master.serialize),
        JField("tracking", tokens.tracking.serialize),
        JField("production", tokens.production.serialize),
        JField("development", tokens.development.serialize)).filter(fieldHasValue))
  }

  implicit val SafeAccountTokensDecomposer: Decomposer[AccountTokens] = new Decomposer[AccountTokens] {
    override def decompose(tokens: AccountTokens): JValue = JObject(
      List(
        JField("production", tokens.production.serialize),
        JField("development", tokens.development.serialize)).filter(fieldHasValue))
  }

  implicit val AccountTokensExtractor: Extractor[AccountTokens] = new Extractor[AccountTokens] with ValidatedExtraction[AccountTokens] {
    override def validated(obj: JValue): Validation[Error, AccountTokens] = (
      (obj \ "production").validated[String] |@|
      (obj \ "development").validated[Option[String]]).apply(AccountTokens((obj \ "master").validated[String] | "", (obj \ "tracking").validated[String] | "", _, _))
  }
}

object AccountTokens extends AccountTokensSerialization

case class AccountId(
  email: String,
  tokens: AccountTokens,
  passwordHash: String)

trait AccountIdSerialization {

  val UnsafeAccountIdDecomposer: Decomposer[AccountId] = new Decomposer[AccountId] {
    override def decompose(id: AccountId): JValue = JObject(
      List(
        JField("email", id.email.serialize),
        JField("tokens", id.tokens.serialize(AccountTokens.UnsafeAccountTokensDecomposer)),
        JField("passwordHash", id.passwordHash.serialize)).filter(fieldHasValue))
  }

  implicit val SafeAccountIdDecomposer: Decomposer[AccountId] = new Decomposer[AccountId] {
    override def decompose(id: AccountId): JValue = JObject(
      List(
        JField("email", id.email.serialize),
        JField("tokens", id.tokens.serialize)).filter(fieldHasValue))
  }

  implicit val AccountIdExtractor: Extractor[AccountId] = new Extractor[AccountId] with ValidatedExtraction[AccountId] {
    override def validated(obj: JValue): Validation[Error, AccountId] = (
      (obj \ "email").validated[String] |@|
      (obj \ "tokens").validated[AccountTokens]).apply(AccountId(_, _, (obj \ "passwordHash").validated[String] | ""))
  }

}

object AccountId extends AccountIdSerialization

case class ContactInformation(
  firstName: String,
  lastName: String,
  company: String,
  title: String,
  phone: String,
  website: String,
  address: Address)

trait ContactInformationSerialization {

  implicit val ContactInformationDecomposer: Decomposer[ContactInformation] = new Decomposer[ContactInformation] {
    override def decompose(contact: ContactInformation): JValue = JObject(
      List(
        JField("firstName", contact.firstName.serialize),
        JField("lastName", contact.lastName.serialize),
        JField("company", contact.company.serialize),
        JField("title", contact.title.serialize),
        JField("phone", contact.phone.serialize),
        JField("website", contact.website.serialize),
        JField("address", contact.address.serialize)).filter(fieldHasValue))
  }

  implicit val ContactInformationExtractor: Extractor[ContactInformation] = new Extractor[ContactInformation] with ValidatedExtraction[ContactInformation] {
    override def validated(obj: JValue): Validation[Error, ContactInformation] = (
      (obj \ "firstName").validated[String] |@|
      (obj \ "lastName").validated[String] |@|
      (obj \ "company").validated[String] |@|
      (obj \ "title").validated[String] |@|
      (obj \ "phone").validated[String] |@|
      (obj \ "website").validated[String] |@|
      (obj \ "address").validated[Address]).apply(ContactInformation(_, _, _, _, _, _, _))
  }

}

object ContactInformation extends ContactInformationSerialization

case class Address(
  street: String,
  city: String, //(region)
  state: String, //(locality)
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
      (obj \ "street").validated[String] |@|
      (obj \ "city").validated[String] |@|
      (obj \ "state").validated[String] |@|
      (obj \ "postalCode").validated[String]).apply(Address(_, _, _, _))
  }

}

object Address extends AddressSerialization

case class ServiceInformation(
  planId: String,
  accountCreated: DateTime,
  credit: Int,
  lastCreditAssessment: DateTime,
  usage: Long,
  lastUsageAssessment: DateTime,
  lastUsageWarning: Option[DateTime],
  lastNoUpgradeWarning: Option[DateTime],
  status: AccountStatus,
  lastAccountStatusChange: Option[DateTime],
  billingDay: Int,
  subscriptionId: Option[String])

trait ServiceInformationSerialization {

  
  implicit val ServiceInformationDecomposer: Decomposer[ServiceInformation] = new Decomposer[ServiceInformation] {
    override def decompose(address: ServiceInformation): JValue = JObject(
      List(
        JField("planId", address.planId.serialize),
        JField("accountCreated", address.accountCreated.serialize),
        JField("credit", address.credit.serialize),
        JField("lastCreditAssessment", address.lastCreditAssessment.serialize),
        JField("usage", address.usage.serialize),
        JField("lastUsageAssessment", address.lastUsageAssessment.serialize),
        JField("lastUsageWarning", address.lastUsageWarning.serialize),
        JField("lastNoUpgradeWarning", address.lastNoUpgradeWarning.serialize),
        JField("status", address.status.serialize),
        JField("lastAccountStatusChange", address.lastAccountStatusChange.serialize),
        JField("billingDay", address.billingDay.serialize),
        JField("subscriptionId", address.subscriptionId.serialize)))
  }

  implicit val ServiceInformationExtractor: Extractor[ServiceInformation] = new Extractor[ServiceInformation] with ValidatedExtraction[ServiceInformation] {

    import braintree.BraintreeUtils.billingDay

    override def validated(obj: JValue): Validation[Error, ServiceInformation] = (
      (obj \ "planId").validated[String] |@|
      (obj \ "accountCreated").validated[DateTime] |@|
      (obj \ "credit").validated[Int] |@|
      (obj \ "lastCreditAssessment").validated[DateTime] |@|
      (obj \ "usage").validated[Long] |@|
      (obj \ "lastUsageWarning").validated[Option[DateTime]] |@|
      (obj \ "lastNoUpgradeWarning").validated[Option[DateTime]] |@|
      (obj \ "status").validated[AccountStatus] |@|
      (obj \ "lastAccountStatusChange").validated[Option[DateTime]] |@|
      (obj \ "subscriptionId").validated[Option[String]]).apply(
            ServiceInformation(
                _, _, _, _, _, 
                (obj \ "lastUsageAssessment").validated[DateTime] | new DateTime(0, DateTimeZone.UTC), 
                _, _, _, _,
                (obj \ "billingDay").validated[Int] | billingDay(new DateTime(0, DateTimeZone.UTC)), 
                _))
  }
}

object ServiceInformation extends ServiceInformationSerialization

case class ValueAccount(
  val id: AccountId,
  val contact: ContactInformation,
  val service: ServiceInformation,
  val billing: Option[BillingInformation]) extends Account

trait AccountSerialization {
  implicit val AccountDecomposer: Decomposer[Account] = new Decomposer[Account] {
    override def decompose(account: Account): JValue = JObject(
      List(
        JField("id", account.id.serialize),
        JField("contact", account.contact.serialize),
        JField("service", account.service.serialize),
        JField("billing", account.billing.serialize)).filter(fieldHasValue))
  }

  implicit val AccountExtractor: Extractor[Account] = new Extractor[Account] with ValidatedExtraction[Account] {
    override def validated(obj: JValue): Validation[Error, Account] = (
      (obj \ "id").validated[AccountId] |@|
      (obj \ "contact").validated[ContactInformation] |@|
      (obj \ "service").validated[ServiceInformation] |@|
      (obj \ "billing").validated[Option[BillingInformation]]).apply(
        ValueAccount(_, _, _, _))
  }
}

object Account extends AccountSerialization

case class AccountInformationUpdate(
    authentication: AccountAuthentication,
    newEmail: Option[String],
    newPassword: Option[Password],
    newPlanId: Option[String],
    newContact: Option[ContactInformation]
)

trait AccountInformationUpdateSerialization {
  implicit val AccountInformationUpdateExtractor: Extractor[AccountInformationUpdate] = new Extractor[AccountInformationUpdate] with ValidatedExtraction[AccountInformationUpdate] {
    override def validated(obj: JValue): Validation[Error, AccountInformationUpdate] = (
      (obj \ "authentication").validated[AccountAuthentication] |@|
      (obj \ "newEmail").validated[Option[String]] |@|
      (obj \ "newPassword").validated[Option[Password]] |@|
      (obj \ "newPlanId").validated[Option[String]] |@|
      (obj \ "newContact").validated[Option[ContactInformation]]).apply(AccountInformationUpdate(_, _, _, _, _))
  
  }    
}

object AccountInformationUpdate extends AccountInformationUpdateSerialization

case class AccountInformation(
  id: AccountId,
  contact: ContactInformation,
  service: ServiceInformation
)

trait AccountInformationSerialization {

  val UnsafeAccountInfoDecomposer: Decomposer[AccountInformation] = new Decomposer[AccountInformation] {
    override def decompose(account: AccountInformation): JValue = JObject(
      List(
        JField("id", account.id.serialize(AccountId.UnsafeAccountIdDecomposer)),
        JField("contact", account.contact.serialize),
        JField("service", account.service.serialize)))
  }

  implicit val AccountInfoDecomposer: Decomposer[AccountInformation] = new Decomposer[AccountInformation] {
    override def decompose(account: AccountInformation): JValue = JObject(
      List(
        JField("id", account.id.serialize),
        JField("contact", account.contact.serialize),
        JField("service", account.service.serialize)))
  }

  implicit val AccountInfoExtractor: Extractor[AccountInformation] = new Extractor[AccountInformation] with ValidatedExtraction[AccountInformation] {
    override def validated(obj: JValue): Validation[Error, AccountInformation] = (
      (obj \ "id").validated[AccountId] |@|
      (obj \ "contact").validated[ContactInformation] |@|
      (obj \ "service").validated[ServiceInformation]).apply(AccountInformation(_, _, _))
  }  
}

object AccountInformation extends AccountInformationSerialization

case class BillingInformationUpdate(
  authentication: AccountAuthentication,
  newBilling: BillingInformation
)

trait BillingInformationUpdateSerialization {
  implicit val BillingInformationUpdateExtractor: Extractor[BillingInformationUpdate] = new Extractor[BillingInformationUpdate] with ValidatedExtraction[BillingInformationUpdate] {
    override def validated(obj: JValue): Validation[Error, BillingInformationUpdate] = (
      (obj \ "authentication").validated[AccountAuthentication] |@|
      (obj \ "newContact").validated[BillingInformation]).apply(BillingInformationUpdate(_, _))
  }      
}

object BillingInformaitonUpdate extends BillingInformationUpdateSerialization

case class BillingInformation(
  cardholder: String,
  number: String,
  expMonth: Int,
  expYear: Int,
  billingPostalCode: String,
  cvv: String) {

  def expDate = expMonth + "/" + expYear

  def safeNumber: String = {
    val show = 4
    val required = 16
    val size = number.length
    val fill = required - (if (size >= show) show else size)
    ("*" * fill) + (if (size >= show) number.substring(size - show) else number)
  }
}

trait BillingInformationSerialization {

  val UnsafeBillingInfoDecomposer: Decomposer[BillingInformation] = new Decomposer[BillingInformation] {
    override def decompose(billing: BillingInformation): JValue = JObject(
      List(
        JField("cardholder", billing.cardholder.serialize),
        JField("number", billing.number.serialize),
        JField("expMonth", billing.expMonth.serialize),
        JField("expYear", billing.expYear.serialize),
        JField("cvv", billing.cvv.serialize),
        JField("billingPostalCode", billing.billingPostalCode.serialize)))
  }
  
  implicit val BillingInformationDecomposer: Decomposer[BillingInformation] = new Decomposer[BillingInformation] {
    override def decompose(billing: BillingInformation): JValue = JObject(
      List(
        JField("cardholder", billing.cardholder.serialize),
        JField("number", billing.safeNumber.serialize),
        JField("expMonth", billing.expMonth.serialize),
        JField("expYear", billing.expYear.serialize),
        JField("billingPostalCode", billing.billingPostalCode.serialize)).filter(fieldHasValue))
  }

  implicit val BillingInformationExtractor: Extractor[BillingInformation] = new Extractor[BillingInformation] with ValidatedExtraction[BillingInformation] {
    override def validated(obj: JValue): Validation[Error, BillingInformation] = (
      (obj \ "cardholder").validated[String] |@|
      (obj \ "number").validated[String] |@|
      (obj \ "expMonth").validated[Int] |@|
      (obj \ "expYear").validated[Int] |@|
      (obj \ "billingPostalCode").validated[String]).apply(
        BillingInformation(_, _, _, _, _, (obj \ "cvv").validated[String] | ""))
  }
}

object BillingInformation extends BillingInformationSerialization
