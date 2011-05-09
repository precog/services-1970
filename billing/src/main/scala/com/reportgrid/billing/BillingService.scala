package com.reportgrid.billing

import blueeyes.BlueEyesServiceBuilder
import blueeyes.core.data.{BijectionsChunkReaderString, BijectionsChunkReaderJson}
import blueeyes.core.http._
import blueeyes.json.JsonAST.{JObject, JField, JValue}
import blueeyes.json.xschema.{Extractor, Decomposer}
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.persistence.mongo._

import net.lag.configgy.ConfigMap
import CreditCard._
import blueeyes.concurrent.Future
import com.braintreegateway.SubscriptionRequest

/**
 * Created by IntelliJ IDEA.
 * User: knuttycombe
 * Date: 5/9/11
 * Time: 10:53 AM
 * To change this template use File | Settings | File Templates.
 */

trait BillingService extends BlueEyesServiceBuilder with BijectionsChunkReaderJson with BijectionsChunkReaderString {
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

        BillingConfig(database, accountsCollection, plansCollection, brainTreeConfig)
      } -> request { state =>
        path("/signup") {
          post { request: HttpRequest[JValue] =>
            HttpResponse[JValue](
              request.content.map(_.deserialize[UserSignup]).map {
                signup => state.billingDatabase(selectOne().from(state.accountsCollection).where(".email" === signup.email)) flatMap {
                  case Some(currentAccount) =>
                    Future.lift(HttpResponse(status = HttpStatus(HttpStatusCodes.BadRequest, "Account already exists.")))

                  case None => createAccount(state, signup)
                }
              } getOrElse {
                Future.lift(HttpResponse(status = HttpStatus(HttpStatusCodes.BadRequest, "No request contents.")))
              }
            )
          }
        }
      } ->  shutdown { state =>
        error("todo")
      }
    }
  }

  def createAccount(config: BillingConfig, signup: UserSignup) = {
    config.billingDatabase(selectOne().from(config.plansCollection).where(".planId" === signup.planId))
    .flatMap {
      _.map(_.deserialize[Plan]).map {
        plan => {
          val request = new SubscriptionRequest().
            id("new_id").
            paymentMethodToken("new_payment_method_token").
            planId(signup.planId).
            merchantAccountId(state.brainTreeConfig.getString("merchant_account_id"))

          val result = gateway.subscription().update(
            "a_subscription_id",
            request
          );

          val account = Account(
            signup.email,
            signup.password,
            plan
          )

          config.billingDatabase(insert(account.serialize)).map {
            _.map {
              _ => HttpResponse(status = HttpStatus(HttpStatusCodes.OK, "Account created."))
            } getOrElse {
              HttpResponse(status = HttpStatus(HttpStatusCodes.FailedDependency, "Unable to create account."))
            }
          }
        }
      } getOrElse {
        Future.lift(HttpResponse(status = HttpStatus(HttpStatusCodes.BadRequest, "No such plan.")))
      }
    }
  }
}

case class BillingConfig(
  billingDatabase: MongoDatabase,
  accountsCollection: MongoCollection,
  plansCollection: MongoCollection,
  brainTreeConfig: ConfigMap
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

case class CreditCard(number: String, cctype: String, expMonth: Int, expYear: Int, ccv: String)

object CreditCard {
  implicit val CreditCardDecomposer : Decomposer[CreditCard] = new Decomposer[CreditCard] {
    override def decompose(cc: CreditCard): JValue =  JObject(
      List(
        JField("number", cc.number.serialize),
        JField("cctype", cc.cctype.serialize),
        JField("expMonth", cc.expMonth.serialize),
        JField("expYear", cc.expYear.serialize),
        JField("ccv", cc.ccv.serialize)
      )
    )
  }

  implicit val CreditCardExtractor : Extractor[CreditCard] = new Extractor[CreditCard] {
    override def extract(obj: JValue) = CreditCard(
      (obj \ "number").deserialize[String],
      (obj \ "cctype").deserialize[String],
      (obj \ "expMonth").deserialize[Int],
      (obj \ "expYear").deserialize[Int],
      (obj \ "ccv").deserialize[String]
    )
  }
}

case class Account(email: String, passwordMD5: String, plan: Plan)

object Account{
  implicit val AccountDecomposer : Decomposer[Account] = new Decomposer[Account] {
    override def decompose(acct: Account): JValue =  JObject(
      List(
        JField("email", acct.email.serialize),
        JField("passwordMD5", acct.passwordMD5.serialize),
        JField("plan", acct.plan.serialize)
      )
    )
  }

  implicit val AccountExtractor : Extractor[Account] = new Extractor[Account] {
    override def extract(obj: JValue) = Account(
      (obj \ "email").deserialize[String],
      (obj \ "passwordMD5").deserialize[String],
      (obj \ "plan").deserialize[Plan]
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
        JField("planId", plan.requestVolume.serialize),
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
      (obj \ "overageRate").deserialize[Option[BigDecimal]]
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
