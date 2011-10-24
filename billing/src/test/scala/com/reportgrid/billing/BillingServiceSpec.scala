package com.reportgrid.billing

import scalaz._
import scalaz.Scalaz._
import scalaz.Validation._
import blueeyes.json.JsonAST._

import blueeyes.concurrent.Future
import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.Extractor._
import Extractor.Error
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.data.ByteChunk
import blueeyes.core.http
import blueeyes.persistence.mongo.MongoQueryBuilder._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.HttpMethods._
import blueeyes.core.service.HttpClient
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.persistence.mongo.MockMongo
import net.lag.configgy.ConfigMap
import com.braintreegateway.BraintreeGateway
import com.reportgrid.billing.braintree.BraintreeService
import com.reportgrid.billing.BillingServiceHandlers._
import com.braintreegateway.Environment
import net.lag.configgy.Configgy
import org.specs.matcher.Matcher

trait TestBillingService extends BlueEyesServiceSpecification with NewBillingService {

  val mongo = new MockMongo

  override def mailerFactory(config: ConfigMap) = new NullMailer
  override def accountsFactory(config: ConfigMap) = {
    val mongo = this.mongo
    val database = mongo.database("test")

    val billingService = braintreeFactory()
    val tokenGenerator = new MockTokenGenerator

    Configgy.configureFromString("""
        accounts {
          credits {
            developer = 25000
          }
        }
    """)
    val cfg = Configgy.config.configMap("accounts")

    new Accounts(cfg, tokenGenerator, billingService, database, "accounts")
  }

  def braintreeFactory(): BraintreeService = {
    val gateway = new BraintreeGateway(
      Environment.SANDBOX,
      "zrfxsyvnfvf8x73f",
      "pyvkc5m7g6bvztfv",
      "zxv9467dx36zkd8y")
    new BraintreeService(gateway, Environment.SANDBOX)
  }

  override def httpClient: HttpClient[ByteChunk] = new HttpClientXLightWeb
}

object BillingServiceSpec extends TestBillingService {

  val validEmail = "a@b.com"
  val validPassword = "abc123"

  val invalidEmail = "notIn@accounts.com"
  val invalidToken = "MISSING_TOKEN"
  val invalidPassword = "notTheOneYouWant"

  val goodBilling = BillingInformation(
    "George Harmon",
    "4111111111111111",
    5,
    2012,
    "123",
    "60607")

  val createAccount1 = CreateAccount(
    Signup(
      validEmail,
      Some("b co"),
      Some("b.co"),
      "starter",
      Some("developer"),
      validPassword),
    None)

  val createAccount2 = CreateAccount(
    Signup(
      "g@h.com",
      None,
      None,
      "bronze",
      None,
      "abc123"),
    Some(goodBilling))

  val billingService = braintreeFactory

  "Billing service" should {
    "handle account creation" in {
      cleanup.before
      cleanup.after
      "succedding with" in {
        "developer credit and no billing" in {
          val t = CreateAccount(
            Signup(
              validEmail,
              Some("b co"),
              Some("b.co"),
              "starter",
              Some("developer"),
              validPassword),
            None)
          val acc = create(t)
          acc must matchCreateAccount(t)
        }
        "developer credit and billing" in {
          val t = CreateAccount(
            Signup(
              validEmail,
              Some("b co"),
              Some("b.co"),
              "starter",
              Some("developer"),
              validPassword),
            Some(BillingInformation(
              "George Harmon",
              "4111111111111111",
              5,
              2012,
              "123",
              "60607")))
          val acc = create(t)
          acc must matchCreateAccount(t)
        }
        "no credit and billing" in {
          val t = CreateAccount(
            Signup(
              validEmail,
              Some("b co"),
              Some("b.co"),
              "starter",
              None,
              validPassword),
            Some(BillingInformation(
              "George Harmon",
              "4111111111111111",
              5,
              2012,
              "123",
              "60607")))
          val acc = create(t)
          acc must matchCreateAccount(t)
        }
      }
      "failing with" in {
        "no credit and no billing" in {
          val t = CreateAccount(
            Signup(
              validEmail,
              Some("b co"),
              Some("b.co"),
              "starter",
              None,
              validPassword),
            None)
          testForCreateError(t, "Unable to create account without account credit or billing information.")
        }
        "bad signup info" in {
          "duplication email address" in {
            accountCount() must beEqual(0)
            create(createAccount1)
            accountCount() must beEqual(1)
            testForCreateError(createAccount1, "An account associated with this email already exists.")
            accountCount() must beEqual(1)
          }
          "bad email" in {
            val t = CreateAccount(
              Signup(
                "notAnEmail",
                Some("b co"),
                Some("b.co"),
                "starter",
                Some("developer"),
                validPassword),
              None)
            testForCreateError(t, "Invalid email address: notAnEmail")
          }
          "bad planId" in {
            val t = CreateAccount(
              Signup(
                validEmail,
                Some("b co"),
                Some("b.co"),
                "nonPlan",
                Some("developer"),
                validPassword),
              None)
            testForCreateError(t, "The selected plan (nonPlan) is not available.")
          }
          "bad password" in {
            val t = CreateAccount(
              Signup(
                validEmail,
                Some("b co"),
                Some("b.co"),
                "starter",
                Some("developer"),
                ""),
              None)
            testForCreateError(t, "Password may not be zero length.")
          }
        }
        "bad billing info" in {

          val signupWithCredit = Signup(
            validEmail,
            Some("b co"),
            Some("b.com"),
            "starter",
            Some("developer"),
            "abc123")

          val signupWithoutCredit = Signup(
            validEmail,
            Some("b co"),
            Some("b.com"),
            "starter",
            Some("developer"),
            "abc123")

          "empty cardholder name" in {
            val b = BillingInformation(
              "",
              goodBilling.number,
              goodBilling.expMonth,
              goodBilling.expYear,
              goodBilling.cvv,
              goodBilling.postalCode)

            val t = CreateAccount(signupWithCredit, Some(b))
            testForCreateError(t, "Cardholder name required.")
          }
          "bad cardnumber" in {
            val b = BillingInformation(
              goodBilling.cardholder,
              "411",
              goodBilling.expMonth,
              goodBilling.expYear,
              goodBilling.cvv,
              goodBilling.postalCode)

            val t = CreateAccount(signupWithCredit, Some(b))
            testForCreateError(t, "Billing errors: Credit card number must be 12-19 digits. Credit card type is not accepted by this merchant account.")
          }
          "bad expiration month" in {
            val b = BillingInformation(
              goodBilling.cardholder,
              goodBilling.number,
              13,
              goodBilling.expYear,
              goodBilling.cvv,
              goodBilling.postalCode)

            val t = CreateAccount(signupWithCredit, Some(b))
            testForCreateError(t, "Billing errors: Expiration date is invalid.")
          }
          "bad expiration year" in {
            val b = BillingInformation(
              goodBilling.cardholder,
              goodBilling.number,
              goodBilling.expMonth,
              -123,
              goodBilling.cvv,
              goodBilling.postalCode)

            val t = CreateAccount(signupWithCredit, Some(b))
            testForCreateError(t, "Billing errors: Expiration date is invalid.")
          }
          "expired card" in {
            // Seems as if the sandbox won't decline an outdated expiration?
            // Fudging it a bit with a credit card number that won't validate...
            val b = BillingInformation(
              goodBilling.cardholder,
              "4000111111111115",
              goodBilling.expMonth,
              2001,
              goodBilling.cvv,
              goodBilling.postalCode)

            val t = CreateAccount(signupWithoutCredit, Some(b))
            testForCreateError(t, "Billing errors:")
          }
          "wrong cvv" in {
            val b = BillingInformation(
              goodBilling.cardholder,
              goodBilling.number,
              goodBilling.expMonth,
              goodBilling.expYear,
              "200",
              goodBilling.postalCode)

            val t = CreateAccount(signupWithoutCredit, Some(b))
            testForCreateError(t, "Billing errors:")
          }
          "cvv not verified" in {
            val b = BillingInformation(
              goodBilling.cardholder,
              goodBilling.number,
              goodBilling.expMonth,
              goodBilling.expYear,
              "201",
              goodBilling.postalCode)

            val t = CreateAccount(signupWithCredit, Some(b))
            testForCreateError(t, "Billing errors:")
          }
          "cvv non participation" in {
            skip("This is not currently rejected by our billing configuration.")
            val b = BillingInformation(
              goodBilling.cardholder,
              goodBilling.number,
              goodBilling.expMonth,
              goodBilling.expYear,
              "301",
              goodBilling.postalCode)

            val t = CreateAccount(signupWithCredit, Some(b))
            testForCreateError(t, "Billing errors: CVV must be 3 or 4 digits.")
          }
          "empty cvv" in {
            val b = BillingInformation(
              goodBilling.cardholder,
              goodBilling.number,
              goodBilling.expMonth,
              goodBilling.expYear,
              "",
              goodBilling.postalCode)

            val t = CreateAccount(signupWithCredit, Some(b))
            testForCreateError(t, "Billing errors: CVV is required.")
          }
          "bad zipcode doesn't match" in {
            val b = BillingInformation(
              goodBilling.cardholder,
              goodBilling.number,
              goodBilling.expMonth,
              goodBilling.expYear,
              goodBilling.cvv,
              "20000")

            val t = CreateAccount(signupWithoutCredit, Some(b))
            testForCreateError(t, "Billing errors:")
          }
          "bad zipcode not verified" in {
            val b = BillingInformation(
              goodBilling.cardholder,
              goodBilling.number,
              goodBilling.expMonth,
              goodBilling.expYear,
              goodBilling.cvv,
              "20001")

            val t = CreateAccount(signupWithoutCredit, Some(b))
            testForCreateError(t, "Billing errors:")
          }
          "empty zipcode" in {
            val b = BillingInformation(
              goodBilling.cardholder,
              goodBilling.number,
              goodBilling.expMonth,
              goodBilling.expYear,
              goodBilling.cvv,
              "")

            val t = CreateAccount(signupWithCredit, Some(b))
            testForCreateError(t, "Postal code required.")
          }
          "verfication error" in {
            skip("This is not currently rejected by our billing configuration.")
            val b = BillingInformation(
              goodBilling.cardholder,
              goodBilling.number,
              goodBilling.expMonth,
              goodBilling.expYear,
              goodBilling.cvv,
              "30000")

            val t = CreateAccount(signupWithoutCredit, Some(b))
            testForCreateError(t, "Test")
          }
          "verification not supported" in {
            skip("This is not currently rejected by our billing configuration.")
            val b = BillingInformation(
              goodBilling.cardholder,
              goodBilling.number,
              goodBilling.expMonth,
              goodBilling.expYear,
              goodBilling.cvv,
              "30001")

            val t = CreateAccount(signupWithoutCredit, Some(b))
            testForCreateError(t, "Test")
          }
        }
      }
    }
    "handle account retieval" in {
      doFirst {
        cleanup
        createTestAccounts
      }
      "succeeding with" in {
        "valid email and password" in {
          val req = AccountAction(Some(validEmail), None, validPassword)
          val res: Future[Option[Account]] = postJsonRequest("/accounts/get", req)
          res.value must eventually(beSomething)
          val con = res.value.get
          con must beSomething
          con.get must matchCreateAccount(createAccount1)
        }
        "valid token and password" in {
          val token = getTokenFor(validEmail, validPassword)
          val req = AccountAction(None, Some(token), validPassword)
          val res: Future[Option[Account]] = postJsonRequest("/accounts/get", req)
          res.value must eventually(beSomething)
          val con = res.value.get
          con must beSomething
          con.get must matchCreateAccount(createAccount1)
        }
        "bad email, valid token and valid password" in {
          val token = getTokenFor(validEmail, validPassword)
          val req = AccountAction(Some(invalidEmail), Some(token), validPassword)
          val res: Future[Option[Account]] = postJsonRequest("/accounts/get", req)
          res.value must eventually(beSomething)
          val con = res.value.get
          con must beSomething
          con.get must matchCreateAccount(createAccount1)
        }
      }
      "failing with" in {
        "valid email, bad token and valid password" in {
          val req = AccountAction(Some(validEmail), Some(invalidToken), validPassword)
          val res: Future[Option[JValue]] = postJsonRequest("/accounts/get", req)
          testFutureOptionError(res, "Account not found.")
        }
        "invalid email" in {
          val req = AccountAction(Some(invalidEmail), None, validPassword)
          val res: Future[Option[JValue]] = postJsonRequest("/accounts/get", req)
          testFutureOptionError(res, "Account not found.")
        }
        "invalid token" in {
          val req = AccountAction(None, Some(invalidToken), validPassword)
          val res: Future[Option[JValue]] = postJsonRequest("/accounts/get", req)
          testFutureOptionError(res, "Account not found.")
        }
        "missing token and missing email" in {
          val req = AccountAction(None, None, validPassword)
          val res: Future[Option[JValue]] = postJsonRequest("/accounts/get", req)
          testFutureOptionError(res, "You must provide a valid token or email.")
        }
        "valid email and incorrect password" in {
          val req = AccountAction(Some(validEmail), None, invalidPassword)
          val res: Future[Option[JValue]] = postJsonRequest("/accounts/get", req)
          testFutureOptionError(res, "You must provide valid identification and authentication information.")
        }
        "valid token and incorrect password" in {
          val token = getTokenFor(validEmail, validPassword)
          val req = AccountAction(None, Some(token), invalidPassword)
          val res: Future[Option[JValue]] = postJsonRequest("/accounts/get", req)
          testFutureOptionError(res, "You must provide valid identification and authentication information.")
        }
      }
      doLast {
        cleanup
      }
    }
    "handle account assessment" in {
      "correctly adjust account credit" in {
        "decrement credit by average daily plan cost" in {
          
        }
        "decrement credit but reduce to no lower than 0" in {
          
        }
        "correctly decrement if last run is more than one day ago" in {
          
        }
        "don't decrement more than once a day" in {
          
        }
      }
      "correctly manage subscriptions" in {
        "if credit is geater than zero stop active subscription" in {
          
        }
        "if credit is zero and billing available start subscription" in {
          
        }
        "if credit is zero and billing not available put account in grace period" in {
          
        }
        "move account from grace period to expried after grace period time elapsed" in {
          
        }
      }
    }
  }

  def getTokenFor(email: String, password: String): String = {
    val req1 = AccountAction(Some(email), None, password)
    val res1: Future[Option[Account]] = postJsonRequest("/accounts/get", req1)
    res1.value.get.get.accountToken
  }

  def putJsonRequest[A, B](url: String, a: A)(implicit d: Decomposer[A], e: Extractor[B]): Future[Option[B]] = {
    val f = service.contentType[JValue](application / json).put[JValue](url)(a.serialize(d))
    f.map { h =>
      h.content.map { c =>
        c.deserialize[B](e)
      }
    }
  }

  def postJsonRequest[A, B](url: String, a: A)(implicit d: Decomposer[A], e: Extractor[B]): Future[Option[B]] = {
    val f = service.contentType[JValue](application / json).post[JValue](url)(a.serialize(d))
    f.map { h =>
      h.content.map { c =>
        c.deserialize[B](e)
      }
    }
  }

  val UnsafeBillingInfoDecomposer: Decomposer[BillingInformation] = new Decomposer[BillingInformation] {
    override def decompose(billing: BillingInformation): JValue = JObject(
      List(
        JField("cardholder", billing.cardholder.serialize),
        JField("number", billing.number.serialize),
        JField("expMonth", billing.expMonth.serialize),
        JField("expYear", billing.expYear.serialize),
        JField("cvv", billing.cvv.serialize),
        JField("postalCode", billing.postalCode.serialize)))
  }

  val UnsafeCreateAccountDecomposer: Decomposer[CreateAccount] = new Decomposer[CreateAccount] {

    import SerializationHelpers._
    override def decompose(account: CreateAccount): JValue = JObject(
      List(
        JField("email", account.signup.email.serialize),
        JField("company", account.signup.company.serialize),
        JField("website", account.signup.website.serialize),
        JField("planId", account.signup.planId.serialize),
        JField("planCreditOption", account.signup.planCreditOption.serialize),
        JField("password", account.signup.password.serialize),
        JField("billing", account.billing.serialize(OptionDecomposer(UnsafeBillingInfoDecomposer)))).filter(fieldHasValue))
  }

  def createResult(a: CreateAccount): Future[Option[JValue]] = {
    putJsonRequest("/accounts/", a)(UnsafeCreateAccountDecomposer, implicitly[Extractor[JValue]])
  }

  def create(a: CreateAccount): Account = {
    val f = putJsonRequest("/accounts/", a)(UnsafeCreateAccountDecomposer, implicitly[Extractor[Account]])
    val value = f.value
    value must eventually(beSomething)
    val option = value.get
    option must beSomething
    option.get
  }

  def cleanup() {
    clearBillingAccounts()
    clearMongo()
  }

  def clearBillingAccounts() {
    billingService.deleteAllCustomers()
  }
  def clearMongo() {
    val db = mongo.database("test")
    val q = remove.from("accounts")
    db(q)
  }

  def createTestAccounts() {
    create(createAccount1)
    create(createAccount2)
  }

  def testFutureOptionError(f: => Future[Option[JValue]], e: String): Unit = {
    f.value must eventually(beSomething)
    val con = f.value.get
    con must beSomething
    con.get must matchErrorString(e)
  }

  def accountCount(): Int = {
    val accounts = accountsFactory(null)
    val f = accounts.findAll
    f.value.get.filter(x => x.isInstanceOf[Success[String, Account]]).size
  }

  def testForCreateError(a: CreateAccount, e: String): Unit = {
    val acc = createResult(a)
    testFutureOptionError(acc, e)
  }

  case class matchErrorString(e: String) extends Matcher[JValue]() {
    def apply(j: => JValue) = {
      j match {
        case JString(a) => beEqual(e)(a)
        case x => (false, "not applicable", "Expected JString but was: " + x)
      }
    }
  }

  case class matchCreateAccount(ca: CreateAccount) extends Matcher[Account]() {
    def apply(v: => Account) = {
      val signup = ca.signup.email == v.email &&
        ca.signup.company == v.company &&
        ca.signup.website == v.website &&
        ca.signup.planId == v.planId
      val cabo = ca.billing
      val vbo = v.billing

      val billing = if (cabo.isDefined && vbo.isDefined) {
        val cab = cabo.get
        val vb = vbo.get
        cab.cardholder == vb.cardholder &&
          cab.expMonth == vb.expMonth &&
          cab.expYear == vb.expYear &&
          cab.postalCode == vb.postalCode
      } else {
        cabo.isEmpty && vbo.isEmpty
      }

      (signup && billing, "Account creation info matches the given account.", "Error account creation info doesn't match the given account.")
    }
  }
}