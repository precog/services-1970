package com.reportgrid.billing

import blueeyes.BlueEyesServiceBuilder
import blueeyes.core.data.{BijectionsChunkString, BijectionsChunkJson}
import blueeyes.core.http._
import blueeyes.json.JsonAST.{JObject, JField, JValue, JNothing}
import blueeyes.json.xschema.{Extractor, Decomposer}
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.persistence.mongo._

import net.lag.configgy.ConfigMap
import CreditCard._
import Account._
import blueeyes.concurrent.Future
import com.braintreegateway._

trait BillingService extends BlueEyesServiceBuilder with BijectionsChunkJson {
  def mongoFactory(configMap: ConfigMap): Mongo

  val billingService = service("billing", "0.01") {
    healthMonitor { monitor => context =>
      startup {
        import context._

        val mongoConfig = config.configMap("mongo")
        val mongo = mongoFactory(mongoConfig)
        val database = mongo.database(mongoConfig.getString("database").getOrElse("billing"))

        val accountsCollection  = mongoConfig.getString("billingAccounts").getOrElse("accounts")
        val plansCollection  = mongoConfig.getString("billingPlans").getOrElse("plans")

        val brainTreeConfig = config.configMap("braintree")

        val gateway = new BraintreeGateway(
            Environment.SANDBOX,
            brainTreeConfig("merchant_id"),
            brainTreeConfig("public_key"),
            brainTreeConfig("private_key")
        )

        BillingConfig(database, accountsCollection, plansCollection, gateway)
      } -> request { state: BillingConfig =>
        jvalue {
          path("/signup") {
            post {
              request: HttpRequest[JValue] => request.content.map(_.deserialize[UserSignup]).map {
                signup => state.billingDatabase(selectOne().from(state.accountsCollection).where(".email" === signup.email)).flatMap[HttpResponse[JValue]] {
                  case Some(currentAccount) =>
                    throw HttpException(HttpStatusCodes.BadRequest, "Account already exists.")

                  case None => for (billingId <- createCustomer(state, signup);
                                    ccId <- createCreditCard(state, signup.cc, billingId);
                                    acct <- createAccount(state, signup, billingId, ccId)) yield acct
                }
              } getOrElse {
                throw HttpException(HttpStatusCodes.BadRequest, "No request contents.")
              }
            }
          }
        }
      } ->  shutdown { state: BillingConfig =>
        error("todo")
      }
    }
  }

  def createCustomer(config: BillingConfig, signup: UserSignup): Future[String] = Future.async {
    val request = (new CustomerRequest).email(signup.email)
    val result = config.gateway.customer.create(request);

    if (result.isSuccess) {
      result.getTarget.getId
    } else {
      error("Could not create customer account.")
    }
  }

  def createCreditCard(config: BillingConfig, cc: CreditCard, customerId: String): Future[String] = Future.async {
    val request = cc.billingAddress.amendRequest(
      (new CreditCardRequest).
      customerId(customerId).
      number(cc.number).
      cvv(cc.cvv).
      expirationDate(cc.expDate).
      cardholderName(cc.cardholder)
    ).done

    val result = config.gateway.creditCard.create(request);

    if (result.isSuccess) {
      result.getTarget.getToken
    } else {
      throw HttpException(HttpStatusCodes.BadRequest, "Could not create customer account; credit card validation may have failed.")
    }
  }

  def createAccount(config: BillingConfig, signup: UserSignup, customerId: String, billingToken: String): Future[HttpResponse[JValue]] = {
    config.billingDatabase(selectOne().from(config.plansCollection).where(".planId" === signup.planId))
    .flatMap[HttpResponse[JValue]] {
      _.map(_.deserialize[Plan]).map {
        plan => {
          val request = (new SubscriptionRequest).
            id("new_id").
            paymentMethodToken(billingToken).
            planId(signup.planId)

          val result = config.gateway.subscription.update(
            "a_subscription_id",
            request
          );

          val account = Account(
            signup.email,
            signup.password,
            plan,
            customerId,
            billingToken
          )

          config.billingDatabase[JNothing.type](insert(account.serialize.asInstanceOf[JObject]).into(config.accountsCollection)).map {
            _.map {
              _ => HttpResponse[JValue](status = HttpStatus(HttpStatusCodes.OK, "Account created."))
            } getOrElse {
              HttpResponse[JValue](status = HttpStatus(HttpStatusCodes.FailedDependency, "Unable to create account."))
            }
          }
        }
      } getOrElse {
        Future.lift(HttpResponse[JValue](status = HttpStatus(HttpStatusCodes.BadRequest, "No such plan.")))
      }
    }
  }
}

case class BillingConfig(
  billingDatabase: MongoDatabase,
  accountsCollection: MongoCollection,
  plansCollection: MongoCollection,
  gateway: BraintreeGateway
)

case class UserSignup(email: String, password: String, planId: String, cc: CreditCard)

object UserSignup{
  implicit val UserSignupDecomposer : Decomposer[UserSignup] = new Decomposer[UserSignup] {
    override def decompose(signup: UserSignup): JValue =  JObject(
      List(
        JField("email", signup.email.serialize),
        JField("password", signup.password.serialize),
        JField("planId", signup.planId.serialize),
        JField("cc", signup.cc.serialize)
      )
    )
  }

  implicit val UserSignupExtractor: Extractor[UserSignup] = new Extractor[UserSignup] {
    override def extract(obj: JValue): UserSignup = UserSignup(
      (obj \ "email").deserialize[String],
      (obj \ "password").deserialize[String],
      (obj \ "planId").deserialize[String],
      (obj \ "cc").deserialize[CreditCard]
    )
  }
}

case class CreditCard(
    cardholder: String,
    number: String,
    cctype: String,
    expMonth: Int,
    expYear: Int,
    cvv: String,
    billingAddress: Address
) {
  def expDate = expMonth + "/" + expYear
}

object CreditCard {
  implicit val CreditCardDecomposer : Decomposer[CreditCard] = new Decomposer[CreditCard] {
    override def decompose(cc: CreditCard): JValue =  JObject(
      List(
        JField("cardholder", cc.cardholder.serialize),
        JField("number", cc.number.serialize),
        JField("cctype", cc.cctype.serialize),
        JField("expMonth", cc.expMonth.serialize),
        JField("expYear", cc.expYear.serialize),
        JField("cvv", cc.cvv.serialize),
        JField("billingAddress", cc.billingAddress.serialize)
      )
    )
  }

  implicit val CreditCardExtractor : Extractor[CreditCard] = new Extractor[CreditCard] {
    override def extract(obj: JValue) = CreditCard(
      (obj \ "cardholder").deserialize[String],
      (obj \ "number").deserialize[String],
      (obj \ "cctype").deserialize[String],
      (obj \ "expMonth").deserialize[Int],
      (obj \ "expYear").deserialize[Int],
      (obj \ "cvv").deserialize[String],
      (obj \ "billingAddress").deserialize[Address]
    )
  }
}

case class Address(
  firstName: String,
  lastName: String,
  company: String,
  streetAddress: String,
  extendedAddress: String,
  locality: String, //city
  region: String, //state
  postalCode: String,
  countryCodeAlpha2: String) {


  def amendRequest(ccr: CreditCardRequest) = ccr.billingAddress.
    firstName(firstName).
    lastName(lastName).
    company(company).
    streetAddress(streetAddress).
    extendedAddress(extendedAddress).
    locality(locality).
    region(region).
    postalCode(postalCode).
    countryCodeAlpha2(countryCodeAlpha2)
}

object Address {
  implicit val AddressDecomposer : Decomposer[Address] = new Decomposer[Address] {
    override def decompose(addr: Address): JValue =  JObject(
      List(
        JField("firstName", addr.firstName.serialize),
        JField("lastName", addr.lastName.serialize),
        JField("company", addr.company.serialize),
        JField("streetAddress", addr.streetAddress.serialize),
        JField("extendedAddress", addr.extendedAddress.serialize),
        JField("locality", addr.locality.serialize),
        JField("region", addr.region.serialize),
        JField("postalCode", addr.postalCode.serialize),
        JField("countryCodeAlpha2", addr.countryCodeAlpha2.serialize)
      )
    )
  }

  implicit val AddressExtractor : Extractor[Address] = new Extractor[Address] {
    override def extract(obj: JValue) = Address(
      (obj \ "firstName").deserialize[String],
      (obj \ "lastName").deserialize[String],
      (obj \ "company").deserialize[String],
      (obj \ "streetAddress").deserialize[String],
      (obj \ "extendedAddress").deserialize[String],
      (obj \ "locality").deserialize[String],
      (obj \ "region").deserialize[String],
      (obj \ "postalCode").deserialize[String],
      (obj \ "countryCodeAlpha2").deserialize[String]
    )
  }
}

case class Account(email: String, passwordMD5: String, plan: Plan, customerId: String, billingToken: String)

object Account{
  implicit val AccountDecomposer : Decomposer[Account] = new Decomposer[Account] {
    override def decompose(acct: Account): JObject = JObject(
      List(
        JField("email", acct.email.serialize),
        JField("passwordMD5", acct.passwordMD5.serialize),
        JField("plan", acct.plan.serialize),
        JField("customerId", acct.customerId.serialize),
        JField("billingToken", acct.billingToken.serialize)
      )
    )
  }

  implicit val AccountExtractor : Extractor[Account] = new Extractor[Account] {
    override def extract(obj: JValue) = Account(
      (obj \ "email").deserialize[String],
      (obj \ "passwordMD5").deserialize[String],
      (obj \ "plan").deserialize[Plan],
      (obj \ "customerId").deserialize[String],
      (obj \ "billingToken").deserialize[String]
    )
  }
}


case class Plan(
  planId: String,
  price: BigDecimal,
  description: String,
  requestLimit: Long,
  upgradePlanId: Option[String],
  overageRate: Option[MeteredRate]
)

object Plan {
  implicit val PlanDecomposer : Decomposer[Plan] = new Decomposer[Plan] {
    override def decompose(plan: Plan): JValue =  JObject(
      List(
        JField("planId", plan.planId.serialize),
        JField("price", plan.price.serialize),
        JField("description", plan.description.serialize),
        JField("requestLimit", plan.requestLimit.serialize),
        JField("upgradePlanId", plan.upgradePlanId.serialize),
        JField("overageRate", plan.overageRate.serialize)
      )
    )
  }

  implicit val PlanExtractor : Extractor[Plan] = new Extractor[Plan] {
    override def extract(obj: JValue) = Plan(
      (obj \ "planId").deserialize[String],
      (obj \ "price").deserialize[BigDecimal],
      (obj \ "description").deserialize[String],
      (obj \ "requestLimit").deserialize[Long],
      (obj \ "upgradePlanId").deserialize[Option[String]],
      (obj \ "overageRate").deserialize[Option[MeteredRate]]
    )
  }
}


case class MeteredRate(requestVolume: Long, price: BigDecimal)

object MeteredRate {
  implicit val MeteredRateDecomposer : Decomposer[MeteredRate] = new Decomposer[MeteredRate] {
    override def decompose(rate: MeteredRate): JValue =  JObject(
      List(
        JField("requestVolume", rate.requestVolume.serialize),
        JField("price", rate.price)
      )
    )
  }

  implicit val MeteredRateExtractor : Extractor[MeteredRate] = new Extractor[MeteredRate] {
    override def extract(obj: JValue) = MeteredRate(
      (obj \ "requestVolume").deserialize[Long],
      (obj \ "price").deserialize[BigDecimal]
    )
  }
}
