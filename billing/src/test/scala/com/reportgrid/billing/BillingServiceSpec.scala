package com.reportgrid.billing

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import scalaz._
import scalaz.Scalaz._
import scalaz.Validation._
import blueeyes.json.JsonAST._

import com.reportgrid.analytics.Token
import blueeyes.concurrent.Future
import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.Extractor._
import Extractor.Error
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.Printer
import blueeyes.core.http.MimeTypes._
import blueeyes.core.data.ByteChunk
import blueeyes.core.http
import blueeyes.persistence.mongo.MongoQueryBuilder._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.HttpMethods._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.core.service.HttpClient
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.core.data.ByteChunk
import blueeyes.persistence.mongo.MockMongo
import net.lag.configgy.ConfigMap
import com.braintreegateway.BraintreeGateway
import com.reportgrid.billing.braintree.BraintreeService
import com.reportgrid.billing.BillingServiceHandlers._
import com.braintreegateway.Environment
import net.lag.configgy.Configgy
import org.specs.matcher.Matcher

trait TestBillingService extends BlueEyesServiceSpecification with BillingService {

  val mongo = new MockMongo

  private val useSendGridTestMailer = false
  
  override def mailerFactory(config: ConfigMap) = {
    if(useSendGridTestMailer) sendGridTestMailer else new NullMailer
  }
  
  def sendGridTestMailer(): Mailer = {
    val client = new HttpClientXLightWeb

    val url = "https://sendgrid.com/api/mail.send.json?"
    val apiUser = "operations@reportgrid.com"
    val apiKey = "seGrid8"  
    
    new OverrideMailTo(Array("nick@reportgrid.com"), new SendGridMailer(httpClient, url, apiUser, apiKey))
  }

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
    "123")

  val goodAddress = Address(
    "123 Street",
    "Broomfield",
    "CO",
    "60607")

  val goodContactInfo = ContactInformation(
    "John",
    "Doe",
    "b co",
    "CEO",
    "411",
    "b.com",
    goodAddress)

  val createAccount1 = CreateAccount(
    validEmail,
    validPassword,
    None,
    "starter",
    Some("developer"),
    goodContactInfo,
    None)

  val createAccount2 = CreateAccount(
    "g@h.com",
    "abc123",
    None,
    "bronze",
    None,
    ContactInformation("", "", "", "", "", "",
      Address("", "", "", "30000")),
    Some(goodBilling))

  val billingService = braintreeFactory

  "Billing service" should {
    "handle account creation" in {
      cleanup.before
      cleanup.after
      "succedding with" in {
        "developer credit and no billing" in {
          val t = createAccount1.copy(planId = "bronze")
          val acc = create(t)
          acc must matchCreateAccount(t)
        }
        "developer credit and billing" in {
          val t = createAccount1.copy(billing = Some(goodBilling))
          val acc = create(t)
          acc must matchCreateAccount(t)
        }
        "no credit and billing" in {
          val t = createAccount1.copy(planCreditOption = None, billing = Some(goodBilling))
          val acc = create(t)
          acc must matchCreateAccount(t)
        }
      }
      "failing with" in {
        "no credit and no billing" in {
          val t = createAccount1.copy(planCreditOption = None)
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
            val t = createAccount1.copy(email = "notAnEmail", planCreditOption = None)
            testForCreateError(t, "Invalid email address: notAnEmail")
          }
          "bad planId" in {
            val t = createAccount1.copy(planId = "nonPlan", planCreditOption = None);
            testForCreateError(t, "The selected plan (nonPlan) is not available.")
          }
          "bad password" in {
            val t = createAccount1.copy(password = "", planCreditOption = None)
            testForCreateError(t, "Password may not be zero length.")
          }
        }
        "bad billing info" in {
          "empty cardholder name" in {
            val b = goodBilling.copy(cardholder = "")
            val t = createAccount1.copy(planCreditOption = None, billing = Some(b))
            testForCreateError(t, "Cardholder name required.")
          }
          "bad cardnumber" in {
            val b = goodBilling.copy(number = "411")
            val t = createAccount1.copy(billing = Some(b))
            testForCreateError(t, "Billing errors: Credit card type is not accepted by this merchant account. Credit card number must be 12-19 digits.")
          }
          "bad expiration month" in {
            val b = goodBilling.copy(expMonth = 13)
            val t = createAccount1.copy(billing = Some(b))              
            testForCreateError(t, "Billing errors: Expiration date is invalid.")
          }
          "bad expiration year" in {
            val b = goodBilling.copy(expYear = -123)
            val t = createAccount1.copy(billing = Some(b))
            testForCreateError(t, "Billing errors: Expiration date is invalid.")
          }
          "expired card" in {
            // Seems as if the sandbox won't decline an outdated expiration?
            // Fudging it a bit with a credit card number that won't validate...
            val b = goodBilling.copy(number = "4000111111111115", expYear = 2001)
            val t = createAccount1.copy(planCreditOption = None, billing = Some(b))
            testForCreateError(t, "Billing errors: Credit card declined. Reason [Do Not Honor]")
          }
          "wrong cvv" in {
            val b = goodBilling.copy(cvv = "200")
            val t = createAccount1.copy(planCreditOption = None, billing = Some(b))
            testForCreateError(t, "Billing errors:")
          }
          "cvv not verified" in {
            val b = goodBilling.copy(cvv = "201")
            val t = createAccount1.copy(billing = Some(b))
            testForCreateError(t, "Billing errors:")
          }
          "cvv non participation" in {
            skip("This is not currently rejected by our billing configuration.")
            val b = goodBilling.copy(cvv = "301")
            val t = createAccount1.copy(billing = Some(b))
            testForCreateError(t, "Billing errors: CVV must be 3 or 4 digits.")
          }
          "empty cvv" in {
            val b = goodBilling.copy(cvv = "")
            val t = createAccount1.copy(billing = Some(b))
            testForCreateError(t, "Billing errors: CVV is required.")
          }
          "bad zipcode doesn't match" in {
            val c = goodContactInfo.copy(address = goodContactInfo.address.copy(postalCode = "20000"))
            val t = createAccount1.copy(planCreditOption = None, contact = c, billing = Some(goodBilling))
            testForCreateError(t, "Billing errors:")
          }
          "bad zipcode not verified" in {
            val c = goodContactInfo.copy(address = goodContactInfo.address.copy(postalCode = "20001"))
            val t = createAccount1.copy(planCreditOption = None, contact = c, billing = Some(goodBilling))
            testForCreateError(t, "Billing errors:")
          }
          "empty zipcode" in {
            val c = goodContactInfo.copy(address = goodContactInfo.address.copy(postalCode = ""))
            val t = createAccount1.copy(contact = c, billing = Some(goodBilling))
            testForCreateError(t, "Postal code required.")
          }
          "verfication error" in {
            skip("This is not currently rejected by our billing configuration.")
            val c = goodContactInfo.copy(address = goodContactInfo.address.copy(postalCode = "30000"))
            val t = createAccount1.copy(planCreditOption = None, contact = c, billing = Some(goodBilling))
            testForCreateError(t, "Test")
          }
          "verification not supported" in {
            skip("This is not currently rejected by our billing configuration.")
            val c = goodContactInfo.copy(address = goodContactInfo.address.copy(postalCode = "30001"))
            val t = createAccount1.copy(planCreditOption = None, contact = c, billing = Some(goodBilling))
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
    
    import AccountStatus._
    
    def newId(id: String): AccountId = AccountId(id, id + "@test.net", "hashOf(" + id + ".password)")

    def newContact(zip: Option[String]): ContactInformation = {
      zip match {
        case Some(z) => ContactInformation(
          "first",
          "last",
          "company",
          "title",
          "phone",
          "website.com",
          Address(
            "street",
            "city",
            "state",
             z))
        case None => ContactInformation(
          "",
          "",
          "",
          "",
          "",
          "",
          Address(
            "",
            "",
            "",
            ""))
      }
    }

    def newService(credit: Int, lastAssessment: DateTime, subsId: Option[String], status: AccountStatus): ServiceInformation = {
      newService2(credit, lastAssessment, subsId, status, None)
    }

    def newService2(credit: Int, lastAssessment: DateTime, subsId: Option[String], status: AccountStatus, gracePeriodExpires: Option[DateTime]): ServiceInformation = {
      ServiceInformation(
        "bronze",
        new DateTime(DateTimeZone.UTC),
        credit,
        lastAssessment,
        0,
        status,
        gracePeriodExpires,
        subsId)
    }

    val now = new DateTime(DateTimeZone.UTC)
    val today = now.toDateMidnight.toDateTime

    "correctly adjust account credit" in {
      "decrement credit by average daily plan cost" in {
        val t = ValueAccount(
          newId("t1"),
          newContact(Some("93449")),
          newService(25000, today.minusDays(1), None, ACTIVE),
          Some(goodBilling))

        createAccount(t)

        val pre = getAccountOrFail(t.id.token)

        pre.service.credit must beEqual(25000)
        pre.service.lastCreditAssessment must beEqual(today.minusDays(1))

        pre.id must matchId(t.id)
        pre.contact must beEqual(t.contact)

        pre.service.planId must beEqual(t.service.planId)
        pre.service.accountCreated must sameTime(t.service.accountCreated)
        pre.service.usage must beEqual(t.service.usage)
        pre.service.status must beEqual(t.service.status)
        pre.service.gracePeriodExpires must sameTimeOption(t.service.gracePeriodExpires)
        pre.service.subscriptionId must beEqual(t.service.subscriptionId)

        pre.billing must matchBilling(t.billing)

        callAndWaitOnAssessment()

        val post = getAccountOrFail(t.id.token)

        // Check account has new credit reduced by the expected amount
        post.service.credit must beEqual(24179)
        // last assessed date is today
        post.service.lastCreditAssessment must beEqual(today)

        // all other attributes remain unchanged
        post.id must matchId(t.id)
        post.contact must beEqual(t.contact)

        post.service.planId must beEqual(t.service.planId)
        post.service.accountCreated must sameTime(t.service.accountCreated)
        post.service.usage must beEqual(t.service.usage)
        post.service.status must beEqual(t.service.status)
        post.service.gracePeriodExpires must sameTimeOption(t.service.gracePeriodExpires)
        post.service.subscriptionId must beEqual(t.service.subscriptionId)

        post.billing must matchBilling(t.billing)
      }
      "decrement credit but reduce to no lower than 0" in {
        // Test account with small credit and last assessed today - 1
        val t = ValueAccount(
          newId("t2"),
          newContact(Some("93449")),
          newService(1, today.minusDays(1), None, ACTIVE),
          Some(goodBilling))

        createAccount(t)

        val pre = getAccountOrFail(t.id.token)

        pre.service.credit must beEqual(1)
        pre.service.lastCreditAssessment must beEqual(today.minusDays(1))

        pre.id must matchId(t.id)
        pre.contact must beEqual(t.contact)

        pre.service.planId must beEqual(t.service.planId)
        pre.service.accountCreated must sameTime(t.service.accountCreated)
        pre.service.usage must beEqual(t.service.usage)
        pre.service.status must beEqual(t.service.status)
        pre.service.gracePeriodExpires must sameTimeOption(t.service.gracePeriodExpires)
        pre.service.subscriptionId must beEqual(t.service.subscriptionId)

        pre.billing must matchBilling(t.billing)

        callAndWaitOnAssessment()

        val post = getAccountOrFail(t.id.token)

        // Check account has new credit reduced by the expected amount
        post.service.credit must beEqual(0)
        // last assessed date is today
        post.service.lastCreditAssessment must beEqual(today)

        // all other attributes remain unchanged
        post.id must matchId(t.id)
        post.contact must beEqual(t.contact)

        post.service.planId must beEqual(t.service.planId)
        post.service.accountCreated must sameTime(t.service.accountCreated)
        post.service.usage must beEqual(t.service.usage)
        post.service.status must beEqual(t.service.status)
        post.service.gracePeriodExpires must sameTimeOption(t.service.gracePeriodExpires)
        post.service.subscriptionId must beSomething

        post.billing must matchBilling(t.billing)
      }
      "correctly decrement if last run is more than one day ago" in {
        // Test account with small credit and last assessed today - 1
        val t = ValueAccount(
          newId("t3"),
          newContact(Some("93449")),
          newService(25000, today.minusDays(2), None, ACTIVE),
          Some(goodBilling))

        createAccount(t)

        val pre = getAccountOrFail(t.id.token)

        pre.service.credit must beEqual(25000)
        pre.service.lastCreditAssessment must beEqual(today.minusDays(2))

        pre.id must matchId(t.id)
        pre.contact must beEqual(t.contact)

        pre.service.planId must beEqual(t.service.planId)
        pre.service.accountCreated must sameTime(t.service.accountCreated)
        pre.service.usage must beEqual(t.service.usage)
        pre.service.status must beEqual(t.service.status)
        pre.service.gracePeriodExpires must sameTimeOption(t.service.gracePeriodExpires)
        pre.service.subscriptionId must beEqual(t.service.subscriptionId)

        pre.billing must matchBilling(t.billing)

        callAndWaitOnAssessment()

        val post = getAccountOrFail(t.id.token)

        // Check account has new credit reduced by the expected amount
        post.service.credit must beEqual(23358)
        // last assessed date is today
        post.service.lastCreditAssessment must beEqual(today)

        // all other attributes remain unchanged
        post.id must matchId(t.id)
        post.contact must beEqual(t.contact)

        post.service.planId must beEqual(t.service.planId)
        post.service.accountCreated must sameTime(t.service.accountCreated)
        post.service.usage must beEqual(t.service.usage)
        post.service.status must beEqual(t.service.status)
        post.service.gracePeriodExpires must sameTimeOption(t.service.gracePeriodExpires)
        post.service.subscriptionId must beEqual(t.service.subscriptionId)

        post.billing must matchBilling(t.billing)
      }
      "don't decrement more than once a day" in {
        val t = ValueAccount(
          newId("t4"),
          newContact(Some("93449")),
          newService(25000, today.minusDays(1), None, ACTIVE),
          Some(goodBilling))

        createAccount(t)

        val pre = getAccountOrFail(t.id.token)

        pre.service.credit must beEqual(25000)
        pre.service.lastCreditAssessment must beEqual(today.minusDays(1))

        pre.id must matchId(t.id)
        pre.contact must beEqual(t.contact)

        pre.service.planId must beEqual(t.service.planId)
        pre.service.accountCreated must sameTime(t.service.accountCreated)
        pre.service.usage must beEqual(t.service.usage)
        pre.service.status must beEqual(t.service.status)
        pre.service.gracePeriodExpires must sameTimeOption(t.service.gracePeriodExpires)
        pre.service.subscriptionId must beEqual(t.service.subscriptionId)

        pre.billing must matchBilling(t.billing)

        callAndWaitOnAssessment()

        val post1 = getAccountOrFail(t.id.token)

        // Check account has new credit reduced by the expected amount
        post1.service.credit must beEqual(24179)
        // last assessed date is today
        post1.service.lastCreditAssessment must beEqual(today)

        // all other attributes remain unchanged
        post1.id must matchId(t.id)
        post1.contact must beEqual(t.contact)

        post1.service.planId must beEqual(t.service.planId)
        post1.service.accountCreated must sameTime(t.service.accountCreated)
        post1.service.usage must beEqual(t.service.usage)
        post1.service.status must beEqual(t.service.status)
        post1.service.gracePeriodExpires must sameTimeOption(t.service.gracePeriodExpires)
        post1.service.subscriptionId must beEqual(t.service.subscriptionId)

        post1.billing must matchBilling(t.billing)

        callAndWaitOnAssessment()

        val post2 = getAccountOrFail(t.id.token)

        // Check account has new credit reduced by the expected amount
        post2.service.credit must beEqual(24179)
        // last assessed date is today
        post2.service.lastCreditAssessment must beEqual(today)

        // all other attributes remain unchanged
        post2.id must matchId(t.id)
        post2.contact must beEqual(t.contact)

        post2.service.planId must beEqual(t.service.planId)
        post2.service.accountCreated must sameTime(t.service.accountCreated)
        post2.service.usage must beEqual(t.service.usage)
        post2.service.status must beEqual(t.service.status)
        post2.service.gracePeriodExpires must sameTimeOption(t.service.gracePeriodExpires)
        post2.service.subscriptionId must beEqual(t.service.subscriptionId)

        post2.billing must matchBilling(t.billing)

      }
    }
    "correctly manage subscriptions" in {
      "if credit is geater than zero stop active subscription" in {

        val t = ValueAccount(
          newId("t5"),
          newContact(Some("93449")),
          newService(25000, today.minusDays(1), Some("willCreateSubscription"), ACTIVE),
          Some(goodBilling))

        createAccount(t)

        val pre = getAccountOrFail(t.id.token)

        pre.service.credit must beEqual(25000)
        pre.service.lastCreditAssessment must beEqual(today.minusDays(1))

        pre.id must matchId(t.id)
        pre.contact must beEqual(t.contact)

        pre.service.planId must beEqual(t.service.planId)
        pre.service.accountCreated must sameTime(t.service.accountCreated)
        pre.service.usage must beEqual(t.service.usage)
        pre.service.status must beEqual(t.service.status)
        pre.service.gracePeriodExpires must sameTimeOption(t.service.gracePeriodExpires)
        pre.service.subscriptionId must beSomething

        pre.billing must matchBilling(t.billing)

        callAndWaitOnAssessment()

        val post = getAccountOrFail(t.id.token)

        // Check account has new credit reduced by the expected amount
        post.service.credit must beEqual(24179)
        // last assessed date is today
        post.service.lastCreditAssessment must beEqual(today)

        // all other attributes remain unchanged
        post.id must matchId(t.id)
        post.contact must beEqual(t.contact)

        post.service.planId must beEqual(t.service.planId)
        post.service.accountCreated must sameTime(t.service.accountCreated)
        post.service.usage must beEqual(t.service.usage)
        post.service.status must beEqual(t.service.status)
        post.service.gracePeriodExpires must sameTimeOption(t.service.gracePeriodExpires)
        post.service.subscriptionId must beNone

        post.billing must matchBilling(t.billing)
      }
      "if credit is zero and billing available start subscription" in {

        val t = ValueAccount(
          newId("t6"),
          newContact(Some("93449")),
          newService(0, today.minusDays(1), None, ACTIVE),
          Some(goodBilling))

        createAccount(t)

        val pre = getAccountOrFail(t.id.token)

        pre.service.credit must beEqual(0)
        pre.service.lastCreditAssessment must beEqual(today.minusDays(1))

        pre.id must matchId(t.id)
        pre.contact must beEqual(t.contact)

        pre.service.planId must beEqual(t.service.planId)
        pre.service.accountCreated must sameTime(t.service.accountCreated)
        pre.service.usage must beEqual(t.service.usage)
        pre.service.status must beEqual(t.service.status)
        pre.service.gracePeriodExpires must sameTimeOption(t.service.gracePeriodExpires)
        pre.service.subscriptionId must beNone

        pre.billing must matchBilling(t.billing)

        callAndWaitOnAssessment()

        val post = getAccountOrFail(t.id.token)

        // Check account has new credit reduced by the expected amount
        post.service.credit must beEqual(0)
        // last assessed date is today
        post.service.lastCreditAssessment must beEqual(today)

        // all other attributes remain unchanged
        post.id must matchId(t.id)
        post.contact must beEqual(t.contact)

        post.service.planId must beEqual(t.service.planId)
        post.service.accountCreated must sameTime(t.service.accountCreated)
        post.service.usage must beEqual(t.service.usage)
        post.service.status must beEqual(t.service.status)
        post.service.gracePeriodExpires must sameTimeOption(t.service.gracePeriodExpires)
        post.service.subscriptionId must beSomething

        post.billing must matchBilling(t.billing)
      }
      "if credit is zero and billing not available put account in grace period" in {
        val t = ValueAccount(
          newId("t7"),
          newContact(Some("93449")),
          newService(0, today.minusDays(1), None, ACTIVE),
          None)

        createAccount(t)

        val pre = getAccountOrFail(t.id.token)

        pre.service.credit must beEqual(0)
        pre.service.lastCreditAssessment must beEqual(today.minusDays(1))

        pre.id must matchId(t.id)
        pre.contact must beEqual(t.contact)

        pre.service.planId must beEqual(t.service.planId)
        pre.service.accountCreated must sameTime(t.service.accountCreated)
        pre.service.usage must beEqual(t.service.usage)
        pre.service.status must beEqual(t.service.status)
        pre.service.gracePeriodExpires must sameTimeOption(t.service.gracePeriodExpires)
        pre.service.subscriptionId must beNone

        pre.billing must matchBilling(t.billing)

        callAndWaitOnAssessment()

        val post = getAccountOrFail(t.id.token)

        // Check account has new credit reduced by the expected amount
        post.service.credit must beEqual(0)
        // last assessed date is today
        post.service.lastCreditAssessment must beEqual(today)

        // all other attributes remain unchanged
        post.id must matchId(t.id)
        post.contact must beEqual(t.contact)

        post.service.planId must beEqual(t.service.planId)
        post.service.accountCreated must sameTime(t.service.accountCreated)
        post.service.usage must beEqual(t.service.usage)
        post.service.status must beEqual(AccountStatus.GRACE_PERIOD)
        post.service.gracePeriodExpires must sameTimeOption(Some(today.plusDays(7)))
        post.service.subscriptionId must beNone

        post.billing must matchBilling(t.billing)
      }
      "move account from grace period to expried after grace period time elapsed" in {
        val t = ValueAccount(
          newId("t8"),
          newContact(Some("93449")),
          newService2(0, today.minusDays(1), None, GRACE_PERIOD, Some(today.minusDays(7))),
          Some(goodBilling))

        createAccount(t)

        val pre = getAccountOrFail(t.id.token)

        pre.service.credit must beEqual(0)
        pre.service.lastCreditAssessment must beEqual(today.minusDays(1))

        pre.id must matchId(t.id)
        pre.contact must beEqual(t.contact)

        pre.service.planId must beEqual(t.service.planId)
        pre.service.accountCreated must sameTime(t.service.accountCreated)
        pre.service.usage must beEqual(t.service.usage)
        pre.service.status must beEqual(t.service.status)
        pre.service.gracePeriodExpires must sameTimeOption(t.service.gracePeriodExpires)
        pre.service.subscriptionId must beNone

        pre.billing must matchBilling(t.billing)

        callAndWaitOnAssessment()

        val post = getAccountOrFail(t.id.token)

        // Check account has new credit reduced by the expected amount
        post.service.credit must beEqual(0)
        // last assessed date is today
        post.service.lastCreditAssessment must beEqual(today)

        // all other attributes remain unchanged
        post.id must matchId(t.id)
        post.contact must beEqual(t.contact)

        post.service.planId must beEqual(t.service.planId)
        post.service.accountCreated must sameTime(t.service.accountCreated)
        post.service.usage must beEqual(t.service.usage)
        post.service.status must beEqual(AccountStatus.DISABLED)
        post.service.gracePeriodExpires must beNone
        post.service.subscriptionId must beNone

        post.billing must matchBilling(t.billing)
      }
    }
  }

  val accounts = accountsFactory(null)

  def createAccount(acc: Account): Unit = {
    accounts.trustedCreateAccount(acc)
  }

  def findAccount(token: String): Future[Validation[String, Account]] = {
    accounts.findByToken(token)
  }

  def getAccountOrFail(token: String): Account = {
    val facc = accounts.findByToken(token)
    val acc = facc.toOption.flatMap(_.toOption)
    acc must beSomething
    acc.get
  }

  def sslHeader = ("ReportGridDecrypter", "")
  
  def callAssessment(sslHeader: (String, String) = sslHeader): Future[Option[Unit]] = {
    val f = service.header(sslHeader)
                    .query("token", Token.Root.tokenId)
                    .contentType[JValue](application / json)
                    .post[JValue]("/accounts/assess")("")
                   
    f.map { h => h.content.map(_ => Unit) }
  }

  def callAndWaitOnAssessment(): Unit = {
    val res = callAssessment()
    val v = res.value
    v must eventually(beSomething)
    v.get must beSomething
  }

  def getTokenFor(email: String, password: String): String = {
    val req1 = AccountAction(Some(email), None, password)
    val res1: Future[Option[Account]] = postJsonRequest("/accounts/get", req1)
    res1.value.get.get.id.token
  }

  def postJsonRequest[A, B](url: String, a: A, sslHeader: (String, String) = sslHeader)(implicit d: Decomposer[A], e: Extractor[B]): Future[Option[B]] = {
    val f = service.header(sslHeader)
                   .contentType[JValue](application / json)
                   .post[JValue](url)(a.serialize(d))
                   
    f.map { h =>
      h.content.map { c =>
        try {
          c.deserialize[B](e)
        } catch {
          case ex => fail("Error deserializing put request to." + ex.getMessage())
        }
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
        JField("cvv", billing.cvv.serialize)))
  }

  val UnsafeCreateAccountDecomposer: Decomposer[CreateAccount] = new Decomposer[CreateAccount] {

    import SerializationHelpers._
    override def decompose(account: CreateAccount): JValue = JObject(
      List(
        JField("email", account.email.serialize),
        JField("password", account.password.serialize),
        JField("confirmPassword", account.confirmPassword.serialize),
        JField("planId", account.planId.serialize),
        JField("planCreditOption", account.planCreditOption.serialize),
        JField("contact", account.contact.serialize),
        JField("billing", account.billing.serialize(OptionDecomposer(UnsafeBillingInfoDecomposer)))).filter(fieldHasValue))
  }

  def createResult(a: CreateAccount): Future[Option[JValue]] = {
    postJsonRequest("/accounts/", a)(UnsafeCreateAccountDecomposer, implicitly[Extractor[JValue]])
  }

  def create(a: CreateAccount): Account = {
    val f = postJsonRequest("/accounts/", a)(UnsafeCreateAccountDecomposer, implicitly[Extractor[Account]])
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
    f.value.toList.flatMap(_.flatMap(_.toOption)).size
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

  case class sameTime(d1: DateTime) extends Matcher[DateTime]() {
    def apply(d2: => DateTime) = {
      (d1.equals(d2), "Dates are equal", "DateTimes not equal " + d1.toString() + " vs " + d2.toString())
    }
  }

  case class matchBilling(b1: Option[BillingInformation]) extends Matcher[Option[BillingInformation]] {
    def apply(b2: => Option[BillingInformation]) = {
      if (b1.isDefined && b2.isDefined) {
        val v1 = b1.get
        val v2 = b2.get

        val test = v1.cardholder == v2.cardholder &&
          v1.number.endsWith(v2.number) &&
          v1.expDate == v2.expDate

        (test, "Billing information is the same", "Billing info differs: " + v1 + " \n vs\n" + v2)
      } else if (b1.isEmpty && b2.isEmpty) {
        (true, "Both are None", "na")
      } else {
        (false, "na", "One billing info is defined the other is not.")
      }
    }
  }

  case class matchId(a1: AccountId) extends Matcher[AccountId] {
    def apply(a2: => AccountId) = {
      val test = a1.email == a2.email &&
                 a1.token == a2.token
      
                 (test, "Ids are compatible", "Ids differ: " + a1 + "\n vs\n" + a2)
    }
  }

  case class sameTimeOption(d1: Option[DateTime]) extends Matcher[Option[DateTime]]() {
    def apply(d2: => Option[DateTime]) = {
      if (d1.isDefined && d2.isDefined) {
        sameTime(d1.get).apply(d2.get)
      } else if (d1.isEmpty && d2.isEmpty) {
        (true, "Both are None", "na")
      } else {
        (false, "na", "One date is defined the other is not.")
      }
    }
  }

  case class matchCreateAccount(ca: CreateAccount) extends Matcher[Account]() {
    def apply(v: => Account) = {

      val signup = ca.email == v.id.email &&
        ca.planId == v.service.planId &&
        ca.contact == v.contact

      val cabo = ca.billing
      val vbo = v.billing

      val billing = if (cabo.isDefined && vbo.isDefined) {
        val cab = cabo.get
        val vb = vbo.get
        cab.cardholder == vb.cardholder &&
          cab.expMonth == vb.expMonth &&
          cab.expYear == vb.expYear
      } else {
        cabo.isEmpty && vbo.isEmpty
      }

      (signup && billing, "Account creation info matches the given account.", "Error account creation info doesn't match the given account.")
    }
  }

  def trustedAccountCreation(acc: Account): Unit = {
    val accounts = accountsFactory(null)
    accounts.trustedCreateAccount(acc)
  }
}