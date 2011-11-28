package com.reportgrid.billing

import org.joda.time.Minutes
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import scalaz._
import scalaz.Scalaz._
import scalaz.Validation._
import blueeyes.json.JsonAST._
import blueeyes.json.JPath

import com.reportgrid.analytics.Token
import blueeyes.concurrent.Future
import blueeyes.concurrent.test.FutureMatchers
import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.Extractor._
import Extractor.Error
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.Printer
import blueeyes.core.http.MimeTypes
import blueeyes.core.http.MimeTypes._
import blueeyes.core.data.ByteChunk
import blueeyes.core.http
import blueeyes.persistence.mongo.MockMongo
import blueeyes.persistence.mongo.RealMongo
import blueeyes.persistence.mongo.IndexType
import blueeyes.persistence.mongo.OrdinaryIndex
import blueeyes.persistence.mongo.MongoQueryBuilder._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.HttpMethods._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.core.service.HttpClient
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.core.data.ByteChunk
import net.lag.configgy.ConfigMap
import com.braintreegateway.BraintreeGateway
import com.reportgrid.billing.braintree.BraintreeService
import com.reportgrid.billing.braintree.BraintreeUtils._
import com.reportgrid.billing.BillingServiceHandlers._
import com.braintreegateway.Environment
import net.lag.configgy.Configgy
import org.specs2.mutable._
import org.specs2.matcher.Matcher
import org.specs2.matcher.MatchResult
import org.specs2.matcher.Expectable
import org.specs2.specification.Scope

import AccountStatus._

trait TestBillingService extends BlueEyesServiceSpecification with BillingService with FutureMatchers with TestHelpers with TestData {

  val sslHeader = ("ReportGridDecrypter", "1")

  val testDatabase = "accounts_test"
  val testCollection = "accounts_test"

  //    Configgy.configureFromString("""
  //          mongo {
  //            database = "accounts_test"
  //            collection = "accounts_test"
  //            servers = ["127.0.0.1:27017"]
  //          }
  //          accounts {
  //            credits {
  //              developer = 25000
  //            }
  //          }
  //      """)
  //  
  //    val mongo = new RealMongo(Configgy.config.configMap("mongo"))

  val mongo = new MockMongo

  private val useSendGridTestMailer = false

  val billingService = braintreeFactory()
  val accounts = accountsFactory(null)

  override def notificationsFactory(config: ConfigMap) = {
    mockNotifications
  }

  def sendGridTestMailer(): Mailer = {
    val client = new HttpClientXLightWeb

    val url = "https://sendgrid.com/api/mail.send.json?"
    val apiUser = "operations@reportgrid.com"
    val apiKey = "seGrid8"

    new OverrideMailTo(Array("nick@reportgrid.com"), new SendGridMailer(httpClient, url, apiUser, apiKey))
  }

  val mockUsageClient = new MockUsageClient

  override def usageClientFactory(config: ConfigMap) = {
    mockUsageClient
  }

  val mockNotifications = new MockNotificationSender

  override def accountsFactory(config: ConfigMap) = {
    val database = mongo.database(testDatabase)
    val tokenGenerator = new MockTokenGenerator

    Configgy.configureFromString("""
        accounts {
          credits {
            developer = 25000
          }
        }
    """)
    val cfg = Configgy.config.configMap("accounts")

    new PrivateAccounts(
        cfg, 
        new MongoAccountInformationStore(database, testCollection), 
        braintreeFactory(),
        mockUsageClient,
        mockNotifications, 
        tokenGenerator)
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

trait TestData {

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
    "60607",
    "123")

  val moreGoodBilling = BillingInformation(
    "John Doe",
    "4111111111111111",
    5,
    2012,
    "60607",
    "456")

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

  val createAccount3 = CreateAccount(
    "john.doe@mail.com",
    "abc123",
    None,
    "bronze",
    None,
    ContactInformation("", "", "", "", "", "",
      Address("", "", "", "30000")),
    Some(moreGoodBilling))

  val createAccounts = createAccount1 :: createAccount2 :: createAccount3 :: Nil
}

class BillingServiceSpec extends TestBillingService {

  trait accountCleanup extends Scope with After {
    def after = cleanup
  }

  trait accountUpdateScope extends Scope with After {
    createAccountSetup
    def after = createAccountTeardown
  }

  "Billing service" should {
    "handle account creation" in {
      "succeeding with" in {
        "developer credit and no billing" in new accountCleanup {
          val t = createAccount1.copy(planId = "bronze")
          val acc = create(t)
          acc must matchAccount(t)
        }
        "developer credit and billing" in new accountCleanup {
          val t = createAccount1.copy(billing = Some(goodBilling))
          val acc = create(t)
          acc must matchAccount(t)
        }
        "no credit and billing" in new accountCleanup {
          val t = createAccount1.copy(planCreditOption = None, billing = Some(goodBilling))
          val acc = create(t)
          acc must matchAccount(t)
        }
      }
      "failing with" in {
        step(cleanup)
        "no credit and no billing" in {
          val t = createAccount1.copy(planCreditOption = None)
          ensureCreateAccountReturnsError(t, "Unable to create account without account credit or billing information.")
        }
        "bad signup info" in {
          "duplication email address" in {
            accountCount() must beEqualTo(0)
            create(createAccount1)
            accountCount() must beEqualTo(1)
            ensureCreateAccountReturnsError(createAccount1, "An account associated with this email already exists.")
            accountCount() must beEqualTo(1)
          }
          step(cleanup)
          "bad email" in {
            val t = createAccount1.copy(email = "notAnEmail", planCreditOption = None)
            ensureCreateAccountReturnsError(t, "Invalid email address: notAnEmail")
          }
          "bad planId" in {
            val t = createAccount1.copy(planId = "nonPlan", planCreditOption = None);
            ensureCreateAccountReturnsError(t, "The selected plan (nonPlan) is not available.")
          }
          "bad password" in {
            val t = createAccount1.copy(password = "", planCreditOption = None)
            ensureCreateAccountReturnsError(t, "Password must be at least five characters long.")
          }
        }
        "bad billing info" in {
          "empty cardholder name" in {
            val b = goodBilling.copy(cardholder = "")
            val t = createAccount1.copy(planCreditOption = None, billing = Some(b))
            ensureCreateAccountReturnsError(t, "Cardholder name required.")
          }
          "bad cardnumber" in {
            val b = goodBilling.copy(number = "411")
            val t = createAccount1.copy(billing = Some(b))
            ensureCreateAccountReturnsError(t, "Billing errors: Credit card type is not accepted by this merchant account. Credit card number must be 12-19 digits.")
          }
          "bad expiration month" in {
            val b = goodBilling.copy(expMonth = 13)
            val t = createAccount1.copy(billing = Some(b))
            ensureCreateAccountReturnsError(t, "Billing errors: Expiration date is invalid.")
          }
          "bad expiration year" in {
            val b = goodBilling.copy(expYear = -123)
            val t = createAccount1.copy(billing = Some(b))
            ensureCreateAccountReturnsError(t, "Billing errors: Expiration date is invalid.")
          }
          "expired card" in {
            // Seems as if the sandbox won't decline an outdated expiration?
            // Fudging it a bit with a credit card number that won't validate...
            val b = goodBilling.copy(number = "4000111111111115", expYear = 2001)
            val t = createAccount1.copy(planCreditOption = None, billing = Some(b))
            ensureCreateAccountReturnsError(t, "Billing errors: Credit card declined. Reason [Do Not Honor]")
          }
          "wrong cvv" in {
            val b = goodBilling.copy(cvv = "200")
            val t = createAccount1.copy(planCreditOption = None, billing = Some(b))
            ensureCreateAccountReturnsError(t, "Billing errors:")
          }
          "cvv not verified" in {
            val b = goodBilling.copy(cvv = "201")
            val t = createAccount1.copy(billing = Some(b))
            ensureCreateAccountReturnsError(t, "Billing errors:")
          }
          //          "cvv non participation" in {
          //            val b = goodBilling.copy(cvv = "301")
          //            val t = createAccount1.copy(billing = Some(b))
          //            ensureCreateAccountReturnsError(t, "Billing errors: CVV must be 3 or 4 digits.")
          //          }
          "empty cvv" in {
            val b = goodBilling.copy(cvv = "")
            val t = createAccount1.copy(billing = Some(b))
            ensureCreateAccountReturnsError(t, "Billing errors: CVV is required.")
          }
          "bad zipcode doesn't match" in {
            val b = goodBilling.copy(billingPostalCode = "20000")
            val t = createAccount1.copy(planCreditOption = None, billing = Some(b))
            ensureCreateAccountReturnsError(t, "Billing errors:")
          }
          "bad zipcode not verified" in {
            val b = goodBilling.copy(billingPostalCode = "20001")
            val t = createAccount1.copy(planCreditOption = None, billing = Some(b))
            ensureCreateAccountReturnsError(t, "Billing errors:")
          }
          "empty zipcode" in {
            val b = goodBilling.copy(billingPostalCode = "")
            val t = createAccount1.copy(planCreditOption = None, billing = Some(b))
            ensureCreateAccountReturnsError(t, "Postal code required.")
          }
          //          "verfication error" in {
          //            skip("This is not currently rejected by our billing configuration.")
          //            val b = goodBilling.copy(billingPostalCode = "30000")
          //            val t = createAccount1.copy(planCreditOption = None, billing = Some(b))
          //            ensureCreateAccountReturnsError(t, "Test")
          //          }
          //          "verification not supported" in {
          //            skip("This is not currently rejected by our billing configuration.")
          //            val b = goodBilling.copy(billingPostalCode = "30001")
          //            val t = createAccount1.copy(planCreditOption = None, billing = Some(b))
          //            ensureCreateAccountReturnsError(t, "Test")
          //          }
        }
      }
    }
    "handle account retrieval" in {
      "handle account information retrieval" in {
        "succeeding with" in {
          "valid email and password" in new accountUpdateScope {
            val req = AccountAuthentication(validEmail, validPassword)
            val res: Future[Option[AccountInformation]] = getAccountInformation(req)
            res must whenDelivered { beSomeAndMatch(matchAccountInfo(createAccount1)) }
          }
        }
        "failing with" in {
          "invalid email" in new accountUpdateScope {
            ensureGetAccountInformationReturnsError(
              AccountAuthentication(invalidEmail, validPassword),
              "Account not found.")
          }
          "valid email and incorrect password" in new accountUpdateScope {
            ensureGetAccountInformationReturnsError(
              AccountAuthentication(validEmail, invalidPassword),
              "You must provide a valid email or account token and a valid password.")
          }
        }
      }
      "handle billing information retrieval" in {

        val emailWithBilling = createAccount2.email

        "succeeding with" in {
          "valid email and password" in new accountUpdateScope {
            val req = AccountAuthentication(emailWithBilling, validPassword)
            val res: Future[Option[BillingInformation]] = getBillingInformation(req)
            res must whenDelivered { beSomeAndMatch(matchBillingInfo(createAccount2)) }
          }
        }
        "failing with" in {
          "invalid email" in new accountUpdateScope {
            ensureGetBillingInformationReturnsError(
              AccountAuthentication(invalidEmail, validPassword),
              "Account not found.")
          }
          "valid email and incorrect password" in new accountUpdateScope {
            ensureGetBillingInformationReturnsError(
              AccountAuthentication(validEmail, invalidPassword),
              "You must provide a valid email or account token and a valid password.")
          }
        }
      }
      "handle account retrieval (legacy)" in {
        "succeeding with" in new accountUpdateScope {
          "valid email and password" in {
            val req = AccountAuthentication(validEmail, validPassword)
            val res: Future[Option[Account]] = getAccount(req)
            res must whenDelivered { beSomeAndMatch(matchAccount(createAccount1)) }
          }
        }
        "failing with" in {
          "invalid email" in new accountUpdateScope {
            ensureGetAccountReturnsError(
              AccountAuthentication(invalidEmail, validPassword),
              "Account not found.")
          }
          "valid email and incorrect password" in new accountUpdateScope {
            ensureGetAccountReturnsError(
              AccountAuthentication(validEmail, invalidPassword),
              "You must provide a valid email or account token and a valid password.")
          }
        }
      }
    }
    "handle account information update" in {
      "succeeding with " in {
        "valid email and password" in new accountUpdateScope {
          val pre = getAccountOrFail(createAccount2.email)
          pre.billing must beSome
          val auth = AccountAuthentication(createAccount2.email, createAccount2.password)
          val req = UpdateAccount(auth, Some("new@email.com"), Some(Password("newPassword", "newPassword")), Some("gold"), Some(createAccount1.contact))
          val res: Future[Option[AccountInformation]] = updateAccountInfo(req)
          val updatedAccount = createAccount2.copy(email = "new@email.com", planId = "gold", contact = createAccount1.contact)
          res must whenDelivered { beSomeAndMatch(matchAccountInfo(updatedAccount)) }
          val post = getAccountOrFail(updatedAccount.email)
          accountDoesNotExist(createAccount2.email)
          post.asAccountInformation must matchAccountInfo(updatedAccount)
          post.billing must beSomeAndMatch(matchBillingInfo(updatedAccount))
        }
        "only new email" in new accountUpdateScope {
          val pre = getAccountOrFail(createAccount2.email)
          pre.billing must beSome
          val auth = AccountAuthentication(createAccount2.email, createAccount2.password)
          val req = UpdateAccount(auth, Some("new@email.com"), None, None, None)
          val res: Future[Option[AccountInformation]] = updateAccountInfo(req)
          val updatedAccount = createAccount2.copy(email = "new@email.com")
          res must whenDelivered { beSomeAndMatch(matchAccountInfo(updatedAccount)) }
          val post = getAccountOrFail(updatedAccount.email)
          accountDoesNotExist(createAccount2.email)
          post.asAccountInformation must matchAccountInfo(updatedAccount)
          post.billing must beSomeAndMatch(matchBillingInfo(updatedAccount))
        }
        "only new password" in new accountUpdateScope {
          val pre = getAccountOrFail(createAccount2.email)
          pre.billing must beSome
          val auth = AccountAuthentication(createAccount2.email, createAccount2.password)
          val req = UpdateAccount(auth, None, Some(Password("newPassword", "newPassword")), None, None)
          val res: Future[Option[AccountInformation]] = updateAccountInfo(req)
          val updatedAccount = createAccount2.copy()
          res must whenDelivered { beSomeAndMatch(matchAccountInfo(updatedAccount)) }
          val post = getAccountOrFail(updatedAccount.email)
          post.asAccountInformation must matchAccountInfo(updatedAccount)
          post.billing must beSomeAndMatch(matchBillingInfo(updatedAccount))
        }
        "only new planId" in new accountUpdateScope {
          val pre = getAccountOrFail(createAccount2.email)
          pre.billing must beSome
          val auth = AccountAuthentication(createAccount2.email, createAccount2.password)
          val req = UpdateAccount(auth, None, None, Some("gold"), None)
          val res: Future[Option[AccountInformation]] = updateAccountInfo(req)
          val updatedAccount = createAccount2.copy(planId = "gold")
          res must whenDelivered { beSomeAndMatch(matchAccountInfo(updatedAccount)) }
          val post = getAccountOrFail(updatedAccount.email)
          post.asAccountInformation must matchAccountInfo(updatedAccount)
          post.billing must beSomeAndMatch(matchBillingInfo(updatedAccount))
        }
        "only new contact" in new accountUpdateScope {
          val pre = getAccountOrFail(createAccount2.email)
          pre.billing must beSome
          val auth = AccountAuthentication(createAccount2.email, createAccount2.password)
          val req = UpdateAccount(auth, None, None, None, Some(createAccount1.contact))
          val res: Future[Option[AccountInformation]] = updateAccountInfo(req)
          val updatedAccount = createAccount2.copy(contact = createAccount1.contact)
          res must whenDelivered { beSomeAndMatch(matchAccountInfo(updatedAccount)) }
          val post = getAccountOrFail(updatedAccount.email)
          post.asAccountInformation must matchAccountInfo(updatedAccount)
          post.billing must beSomeAndMatch(matchBillingInfo(updatedAccount))
        }
      }
      "failing with " in {
        "bad email" in new accountUpdateScope {
          accountsUnchanged()
          val auth = AccountAuthentication(invalidEmail, createAccount1.password)
          val req = UpdateAccount(auth, Some("new@email.com"), Some(Password("newPassword", "newPassword")), Some("gold"), Some(createAccount1.contact))
          ensureUpdateAccountInfoReturnsError(req, "Account not found.")
          accountsUnchanged()
        }
        "bad password" in new accountUpdateScope {
          accountsUnchanged()
          val auth = AccountAuthentication(createAccount1.email, invalidPassword)
          val req = UpdateAccount(auth, Some("new@email.com"), Some(Password("newPassword", "newPassword")), Some("gold"), Some(createAccount1.contact))
          ensureUpdateAccountInfoReturnsError(req, "You must provide a valid email or account token and a valid password.")
          accountsUnchanged()
        }
      }
    }
    "handle close account" in {
      "succeeding with " in {
        "valid email and password" in new accountUpdateScope {
          val pre = getAccountOrFail(createAccount2.email)
          pre.billing must beSome
          val req = AccountAuthentication(createAccount2.email, createAccount2.password)
          val res: Future[Option[Account]] = closeAccount(req)
          res must whenDelivered { beSomeAndMatch(matchAccount(createAccount2)) }
          val post = getAccountOrFail(createAccount2.email)
          post.asAccountInformation must matchAccountInfo(createAccount2)
          post.billing must beNone
          post must beDisabled()
        }
      }
      "failing with " in {
        "bad email" in new accountUpdateScope {
          accountsUnchanged()
          val req = AccountAuthentication(invalidEmail, createAccount1.password)
          ensureCloseAccountReturnsError(req, "Account not found.")
          accountsUnchanged()
        }
        "bad password" in new accountUpdateScope {
          accountsUnchanged()
          val req = AccountAuthentication(createAccount1.email, invalidPassword)
          ensureCloseAccountReturnsError(req, "You must provide a valid email or account token and a valid password.")
          accountsUnchanged()
        }
      }
    }
    "handle billing information update" in {
      "succeeding with no billing present and " in {
        "valid email and password" in new accountUpdateScope {
          val pre = getAccountOrFail(createAccount1.email)
          pre.billing must beNone
          val billingUpdate = UpdateBilling(AccountAuthentication(createAccount1.email, createAccount1.password), goodBilling)
          val res: Future[Option[BillingInformation]] =
            updateBilling(billingUpdate)(UpdateBilling.UnsafeUpdateBillingDecomposer, implicitly[Extractor[BillingInformation]])
          res must whenDelivered { beSomeAndMatch(matchBillingInfo(createAccount2)) }
          val post = getAccountOrFail(createAccount1.email)
          post.billing must beSomeAndMatch(matchBillingInfo(createAccount2))
        }
      }
      "succeeding with billing present and " in {
        "valid email and password" in new accountUpdateScope {
          val pre = getAccountOrFail(createAccount2.email)
          pre.billing must beSomeAndMatch(matchBillingInfo(createAccount2))
          val billingUpdate = UpdateBilling(AccountAuthentication(createAccount2.email, createAccount2.password), moreGoodBilling)
          val res: Future[Option[BillingInformation]] =
            updateBilling(billingUpdate)(UpdateBilling.UnsafeUpdateBillingDecomposer, implicitly[Extractor[BillingInformation]])
          res must whenDelivered { beSomeAndMatch(matchBillingInfo(createAccount3)) }
          val post = getAccountOrFail(createAccount2.email)
          post.billing must beSomeAndMatch(matchBillingInfo(createAccount3))
        }
      }
      "failing with " in {
        "bad email" in new accountUpdateScope {
          accountsUnchanged()
          val billingUpdate = UpdateBilling(AccountAuthentication(invalidEmail, createAccount1.password), moreGoodBilling)
          ensureUpdateBillingReturnsError(billingUpdate, "Account not found.")
          accountsUnchanged()
        }
        "bad password" in new accountUpdateScope {
          accountsUnchanged()
          val billingUpdate = UpdateBilling(AccountAuthentication(createAccount1.email, invalidPassword), moreGoodBilling)
          ensureUpdateBillingReturnsError(billingUpdate, "You must provide a valid email or account token and a valid password.")
          accountsUnchanged()
        }
      }
    }
    "handle billing information removal" in {
      "succeeding with billing present and " in {
        "valid email and password" in new accountUpdateScope {
          val pre = getAccountOrFail(createAccount2.email)
          pre.billing must beSome
          val req = AccountAuthentication(createAccount2.email, createAccount2.password)
          val res: Future[Option[BillingInformation]] = removeBilling(req)
          res must whenDelivered { beSomeAndMatch(matchBillingInfo(createAccount2)) }
          val post = getAccountOrFail(createAccount2.email)
          post.billing must beNone
        }
      }
      "succeeding with no billing present and " in {
        "valid email and password" in new accountUpdateScope {
          val pre = getAccountOrFail(createAccount1.email)
          pre.billing must beNone
          val req = AccountAuthentication(createAccount1.email, createAccount1.password)
          val res: Future[Option[Option[BillingInformation]]] = removeBilling(req)
          res must whenDelivered { beSome(None) }
          val post = getAccountOrFail(createAccount1.email)
          post.billing must beNone
        }
      }
      "failing with " in {
        "bad email" in new accountUpdateScope {
          accountsUnchanged()
          val req = AccountAuthentication(invalidEmail, createAccount1.password)
          ensureRemoveBillingReturnsError(req, "Account not found.")
          accountsUnchanged()
        }
        "bad password" in new accountUpdateScope {
          accountsUnchanged()
          val req = AccountAuthentication(createAccount1.email, invalidPassword)
          ensureRemoveBillingReturnsError(req, "You must provide a valid email or account token and a valid password.")
          accountsUnchanged()
        }
      }
    }
    "handle credit accounting" in {

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

          val pre = getAccountOrFail(t.id.email)

          pre.service.credit must beEqualTo(25000)
          pre.service.lastCreditAssessment must beEqualTo(today.minusDays(1))

          pre.id must matchId(t.id)
          pre.contact must beEqualTo(t.contact)

          pre.service.planId must beEqualTo(t.service.planId)
          pre.service.accountCreated must sameTime(t.service.accountCreated)
          pre.service.usage must beEqualTo(t.service.usage)
          pre.service.status must beEqualTo(t.service.status)
          pre.service.lastAccountStatusChange must beNone
          pre.service.subscriptionId must beEqualTo(t.service.subscriptionId)

          pre.billing must matchBilling(t.billing)

          callAndWaitOnCreditAccounting()

          val post = getAccountOrFail(t.id.email)

          // Check account has new credit reduced by the expected amount
          post.service.credit must beEqualTo(24179)
          // last assessed date is today
          post.service.lastCreditAssessment must beEqualTo(today)

          // all other attributes remain unchanged
          post.id must matchId(t.id)
          post.contact must beEqualTo(t.contact)

          post.service.planId must beEqualTo(t.service.planId)
          post.service.accountCreated must sameTime(t.service.accountCreated)
          post.service.usage must beEqualTo(t.service.usage)
          post.service.status must beEqualTo(t.service.status)
          post.service.lastAccountStatusChange must beNone
          post.service.subscriptionId must beEqualTo(t.service.subscriptionId)

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

          val pre = getAccountOrFail(t.id.email)

          pre.service.credit must beEqualTo(1)
          pre.service.lastCreditAssessment must beEqualTo(today.minusDays(1))

          pre.id must matchId(t.id)
          pre.contact must beEqualTo(t.contact)

          pre.service.planId must beEqualTo(t.service.planId)
          pre.service.accountCreated must sameTime(t.service.accountCreated)
          pre.service.usage must beEqualTo(t.service.usage)
          pre.service.status must beEqualTo(t.service.status)
          pre.service.lastAccountStatusChange must beNone
          pre.service.subscriptionId must beEqualTo(t.service.subscriptionId)

          pre.billing must matchBilling(t.billing)

          callAndWaitOnCreditAccounting()

          val post = getAccountOrFail(t.id.email)

          // Check account has new credit reduced by the expected amount
          post.service.credit must beEqualTo(0)
          // last assessed date is today
          post.service.lastCreditAssessment must beEqualTo(today)

          // all other attributes remain unchanged
          post.id must matchId(t.id)
          post.contact must beEqualTo(t.contact)

          post.service.planId must beEqualTo(t.service.planId)
          post.service.accountCreated must sameTime(t.service.accountCreated)
          post.service.usage must beEqualTo(t.service.usage)
          post.service.status must beEqualTo(t.service.status)
          post.service.lastAccountStatusChange must beNone
          post.service.subscriptionId must beSome

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

          val pre = getAccountOrFail(t.id.email)

          pre.service.credit must beEqualTo(25000)
          pre.service.lastCreditAssessment must beEqualTo(today.minusDays(2))

          pre.id must matchId(t.id)
          pre.contact must beEqualTo(t.contact)

          pre.service.planId must beEqualTo(t.service.planId)
          pre.service.accountCreated must sameTime(t.service.accountCreated)
          pre.service.usage must beEqualTo(t.service.usage)
          pre.service.status must beEqualTo(t.service.status)
          pre.service.lastAccountStatusChange must beNone
          pre.service.subscriptionId must beEqualTo(t.service.subscriptionId)

          pre.billing must matchBilling(t.billing)

          callAndWaitOnCreditAccounting()

          val post = getAccountOrFail(t.id.email)

          // Check account has new credit reduced by the expected amount
          post.service.credit must beEqualTo(23358)
          // last assessed date is today
          post.service.lastCreditAssessment must beEqualTo(today)

          // all other attributes remain unchanged
          post.id must matchId(t.id)
          post.contact must beEqualTo(t.contact)

          post.service.planId must beEqualTo(t.service.planId)
          post.service.accountCreated must sameTime(t.service.accountCreated)
          post.service.usage must beEqualTo(t.service.usage)
          post.service.status must beEqualTo(t.service.status)
          post.service.lastAccountStatusChange must beNone
          post.service.subscriptionId must beEqualTo(t.service.subscriptionId)

          post.billing must matchBilling(t.billing)
        }
        "don't decrement more than once a day" in {
          val t = ValueAccount(
            newId("t4"),
            newContact(Some("93449")),
            newService(25000, today.minusDays(1), None, ACTIVE),
            Some(goodBilling))

          createAccount(t)

          val pre = getAccountOrFail(t.id.email)

          pre.service.credit must beEqualTo(25000)
          pre.service.lastCreditAssessment must beEqualTo(today.minusDays(1))

          pre.id must matchId(t.id)
          pre.contact must beEqualTo(t.contact)

          pre.service.planId must beEqualTo(t.service.planId)
          pre.service.accountCreated must sameTime(t.service.accountCreated)
          pre.service.usage must beEqualTo(t.service.usage)
          pre.service.status must beEqualTo(t.service.status)
          pre.service.lastAccountStatusChange must beNone
          pre.service.subscriptionId must beEqualTo(t.service.subscriptionId)

          pre.billing must matchBilling(t.billing)

          callAndWaitOnCreditAccounting()

          val post1 = getAccountOrFail(t.id.email)

          // Check account has new credit reduced by the expected amount
          post1.service.credit must beEqualTo(24179)
          // last assessed date is today
          post1.service.lastCreditAssessment must beEqualTo(today)

          // all other attributes remain unchanged
          post1.id must matchId(t.id)
          post1.contact must beEqualTo(t.contact)

          post1.service.planId must beEqualTo(t.service.planId)
          post1.service.accountCreated must sameTime(t.service.accountCreated)
          post1.service.usage must beEqualTo(t.service.usage)
          post1.service.status must beEqualTo(t.service.status)
          post1.service.lastAccountStatusChange must beNone
          post1.service.subscriptionId must beEqualTo(t.service.subscriptionId)

          post1.billing must matchBilling(t.billing)

          callAndWaitOnCreditAccounting()

          val post2 = getAccountOrFail(t.id.email)

          // Check account has new credit reduced by the expected amount
          post2.service.credit must beEqualTo(24179)
          // last assessed date is today
          post2.service.lastCreditAssessment must beEqualTo(today)

          // all other attributes remain unchanged
          post2.id must matchId(t.id)
          post2.contact must beEqualTo(t.contact)

          post2.service.planId must beEqualTo(t.service.planId)
          post2.service.accountCreated must sameTime(t.service.accountCreated)
          post2.service.usage must beEqualTo(t.service.usage)
          post2.service.status must beEqualTo(t.service.status)
          post2.service.lastAccountStatusChange must beNone
          post2.service.subscriptionId must beEqualTo(t.service.subscriptionId)

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

          val pre = getAccountOrFail(t.id.email)

          pre.service.credit must beEqualTo(25000)
          pre.service.lastCreditAssessment must beEqualTo(today.minusDays(1))

          pre.id must matchId(t.id)
          pre.contact must beEqualTo(t.contact)

          pre.service.planId must beEqualTo(t.service.planId)
          pre.service.accountCreated must sameTime(t.service.accountCreated)
          pre.service.usage must beEqualTo(t.service.usage)
          pre.service.status must beEqualTo(t.service.status)
          pre.service.lastAccountStatusChange must beNone
          pre.service.subscriptionId must beSome

          pre.billing must matchBilling(t.billing)

          callAndWaitOnCreditAccounting()

          val post = getAccountOrFail(t.id.email)

          // Check account has new credit reduced by the expected amount
          post.service.credit must beEqualTo(24179)
          // last assessed date is today
          post.service.lastCreditAssessment must beEqualTo(today)

          // all other attributes remain unchanged
          post.id must matchId(t.id)
          post.contact must beEqualTo(t.contact)

          post.service.planId must beEqualTo(t.service.planId)
          post.service.accountCreated must sameTime(t.service.accountCreated)
          post.service.usage must beEqualTo(t.service.usage)
          post.service.status must beEqualTo(t.service.status)
          post.service.lastAccountStatusChange must beNone
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

          val pre = getAccountOrFail(t.id.email)

          pre.service.credit must beEqualTo(0)
          pre.service.lastCreditAssessment must beEqualTo(today.minusDays(1))

          pre.id must matchId(t.id)
          pre.contact must beEqualTo(t.contact)

          pre.service.planId must beEqualTo(t.service.planId)
          pre.service.accountCreated must sameTime(t.service.accountCreated)
          pre.service.usage must beEqualTo(t.service.usage)
          pre.service.status must beEqualTo(t.service.status)
          pre.service.lastAccountStatusChange must beNone
          pre.service.subscriptionId must beNone

          pre.billing must matchBilling(t.billing)

          callAndWaitOnCreditAccounting()

          val post = getAccountOrFail(t.id.email)

          // Check account has new credit reduced by the expected amount
          post.service.credit must beEqualTo(0)
          // last assessed date is today
          post.service.lastCreditAssessment must beEqualTo(today)

          // all other attributes remain unchanged
          post.id must matchId(t.id)
          post.contact must beEqualTo(t.contact)

          post.service.planId must beEqualTo(t.service.planId)
          post.service.accountCreated must sameTime(t.service.accountCreated)
          post.service.usage must beEqualTo(t.service.usage)
          post.service.status must beEqualTo(t.service.status)
          post.service.lastAccountStatusChange must beNone
          post.service.subscriptionId must beSome

          post.billing must matchBilling(t.billing)
        }
        "if credit is zero and billing not available disable account" in {
          val t = ValueAccount(
            newId("t7"),
            newContact(Some("93449")),
            newService(0, today.minusDays(1), None, ACTIVE),
            None)

          createAccount(t)

          val pre = getAccountOrFail(t.id.email)

          pre.service.credit must beEqualTo(0)
          pre.service.lastCreditAssessment must beEqualTo(today.minusDays(1))

          pre.id must matchId(t.id)
          pre.contact must beEqualTo(t.contact)

          pre.service.planId must beEqualTo(t.service.planId)
          pre.service.accountCreated must sameTime(t.service.accountCreated)
          pre.service.usage must beEqualTo(t.service.usage)
          pre.service.status must beEqualTo(t.service.status)
          pre.service.lastAccountStatusChange must beNone
          pre.service.subscriptionId must beNone

          pre.billing must matchBilling(t.billing)

          callAndWaitOnCreditAccounting()

          val post = getAccountOrFail(t.id.email)

          // Check account has new credit reduced by the expected amount
          post.service.credit must beEqualTo(0)
          // last assessed date is today
          post.service.lastCreditAssessment must beEqualTo(today)

          // all other attributes remain unchanged
          post.id must matchId(t.id)
          post.contact must beEqualTo(t.contact)

          post.service.planId must beEqualTo(t.service.planId)
          post.service.accountCreated must sameTime(t.service.accountCreated)
          post.service.usage must beEqualTo(t.service.usage)
          post.service.status must beEqualTo(AccountStatus.DISABLED)
          post.service.lastAccountStatusChange must beSome
          post.service.subscriptionId must beNone

          post.billing must matchBilling(t.billing)
        }
      }
    }
    "handle usage accounting " in {
      trait accountCleanup extends Scope with After {
        def after = cleanup
      }

      "accumulate usage in normal case" in new accountCleanup {
        val now = new DateTime(DateTimeZone.UTC)

        val usage = 1001

        mockUsageClient.default = Some(usage)

        // create test account
        val t = ValueAccount(
          newId("u1"),
          newContact(Some("93449")),
          newService(0, now, now.minusMinutes(10), None, ACTIVE, None),
          Some(goodBilling))

        createAccount(t)
        mockNotifications.notifications.clear

        // run assessment
        callAndWaitOnUsageAccounting()

        // check results
        val post = getAccountOrFail(t.id.email)

        val startDate = billingStartDate(now, t.service.billingDay)
        val minutes = Minutes.minutesBetween(startDate, now).getMinutes

        post.service.usage must_== 1001
        Minutes.minutesBetween(post.service.lastUsageAssessment, now).getMinutes must_== 0

        mockNotifications.notifications.size must_== 1
        mockNotifications.notifications(0).isInstanceOf[AdminNotification] must beTrue

        val expected = post.service.copy(usage = 0, lastUsageAssessment = now.minusMinutes(10))

        expected must_== t.service
      }
      "notify of usage rate to exceed monthly quota and outside the quiet period" in new accountCleanup {

        val now = new DateTime(DateTimeZone.UTC)

        val usage = 99999

        mockUsageClient.default = Some(usage)

        // create test account
        val t = ValueAccount(
          newId("u2"),
          newContact(Some("93449")),
          newService(0, now, now.minusMinutes(10), None, ACTIVE, None),
          Some(goodBilling))

        val targetDayOfMonth = now.getDayOfMonth() match {
          case x if x >= 28 => 1
          case x => x + 1
        }

        val tt = t.copy(service = t.service.copy(billingDay = targetDayOfMonth, planId = "starter"))

        createAccount(tt)
        mockNotifications.notifications.clear

        // run assessment
        callAndWaitOnUsageAccounting()

        // check results
        val post = getAccountOrFail(tt.id.email)

        post.service.usage must_== usage
        post.service.lastUsageWarning must beSome
        Minutes.minutesBetween(post.service.lastUsageAssessment, now).getMinutes must_== 0

        mockNotifications.notifications.size must_== 2
        mockNotifications.notifications(0).isInstanceOf[UpgradeWarning] must beTrue
        mockNotifications.notifications(1).isInstanceOf[AdminNotification] must beTrue

        val expected = post.service.copy(usage = 0, lastUsageAssessment = now.minusMinutes(10), lastUsageWarning = None)

        expected must_== tt.service
      }
      "quota notification skipped when already sent this billing cycle" in new accountCleanup {

        val now = new DateTime(DateTimeZone.UTC)

        val usage = 99999

        mockUsageClient.default = Some(usage)

        // create test account
        val t = ValueAccount(
          newId("u2"),
          newContact(Some("93449")),
          newService(0, now, now.minusMinutes(10), None, ACTIVE, None),
          Some(goodBilling))

        val targetDayOfMonth = now.getDayOfMonth() match {
          case x if x >= 28 => 1
          case x => x + 1
        }

        val tt = t.copy(service = t.service.copy(billingDay = targetDayOfMonth, planId = "starter", lastUsageWarning = Some(now.minusDays(1))))

        createAccount(tt)
        mockNotifications.notifications.clear

        // run assessment
        callAndWaitOnUsageAccounting()

        // check results
        val post = getAccountOrFail(tt.id.email)

        post.service.usage must_== usage

        mockNotifications.notifications.size must_== 1
        mockNotifications.notifications(0).isInstanceOf[AdminNotification] must beTrue

        val expected = post.service.copy(usage = 0, lastUsageAssessment = now.minusMinutes(10))

        expected must_== tt.service
      }
      "no action inside quota quiet period" in new accountCleanup {

        val now = new DateTime(DateTimeZone.UTC)

        val usage = 99999

        mockUsageClient.default = Some(usage)

        // create test account
        val t = ValueAccount(
          newId("u3"),
          newContact(Some("93449")),
          newService(0, now, now.minusMinutes(10), None, ACTIVE, None),
          Some(goodBilling))

        val targetDayOfMonth = now.getDayOfMonth() match {
          case 1 => 31
          case x if x >= 28 => 27
          case x => x - 1
        }

        val tt = t.copy(service = t.service.copy(billingDay = targetDayOfMonth, planId = "starter"))

        createAccount(tt)
        mockNotifications.notifications.clear

        // run assessment
        callAndWaitOnUsageAccounting()

        // check results
        val post = getAccountOrFail(tt.id.email)

        post.service.usage must_== usage
        Minutes.minutesBetween(post.service.lastUsageAssessment, now).getMinutes must_== 0

        mockNotifications.notifications.size must_== 1
        mockNotifications.notifications(0).isInstanceOf[AdminNotification] must beTrue

        val expected = post.service.copy(usage = 0, lastUsageAssessment = now.minusMinutes(10))

        expected must_== tt.service
      }
      "automatically upgrade plan if usage exceeds monthly quota" in new accountCleanup {

        val now = new DateTime(DateTimeZone.UTC)

        val usage = 100001

        mockUsageClient.default = Some(usage)

        // create test account
        val t = ValueAccount(
          newId("u4"),
          newContact(Some("93449")),
          newService(0, now, now.minusMinutes(10), None, ACTIVE, None),
          Some(goodBilling))

        val targetDayOfMonth = now.getDayOfMonth() match {
          case x if x >= 28 => 1
          case x => x + 1
        }

        val tt = t.copy(service = t.service.copy(billingDay = targetDayOfMonth, planId = "starter", lastUsageWarning = Some(now)))

        createAccount(tt)
        mockNotifications.notifications.clear

        // run assessment
        callAndWaitOnUsageAccounting()

        // check results
        val post = getAccountOrFail(tt.id.email)

        post.service.usage must_== usage
        post.service.planId must_== "bronze"
        Minutes.minutesBetween(post.service.lastUsageAssessment, now).getMinutes must_== 0

        mockNotifications.notifications.size must_== 2
        mockNotifications.notifications(0).isInstanceOf[UpgradeConfirmation] must beTrue
        mockNotifications.notifications(1).isInstanceOf[AdminNotification] must beTrue

        val expected = post.service.copy(usage = 0, planId = "starter", lastUsageAssessment = now.minusMinutes(10))

        expected must_== tt.service
      }
      "notify user of no viable upgrade path" in new accountCleanup {

        val now = new DateTime(DateTimeZone.UTC)

        val usage = 13000001

        mockUsageClient.default = Some(usage)

        // create test account
        val t = ValueAccount(
          newId("u5"),
          newContact(Some("93449")),
          newService(0, now, now.minusMinutes(10), None, ACTIVE, None),
          Some(goodBilling))

        val targetDayOfMonth = now.getDayOfMonth() match {
          case x if x >= 28 => 1
          case x => x + 1
        }

        val tt = t.copy(service = t.service.copy(billingDay = targetDayOfMonth, planId = "gold", lastUsageWarning = Some(now)))

        createAccount(tt)

        mockNotifications.notifications.clear

        // run assessment
        callAndWaitOnUsageAccounting()

        // check results
        val post = getAccountOrFail(tt.id.email)

        post.service.usage must_== usage
        post.service.planId must_== "gold"
        post.service.lastNoUpgradeWarning must beSome
        Minutes.minutesBetween(post.service.lastUsageAssessment, now).getMinutes must_== 0

        mockNotifications.notifications.size must_== 2
        mockNotifications.notifications(0).isInstanceOf[NoUpgradeWarning] must beTrue
        mockNotifications.notifications(1).isInstanceOf[AdminNotification] must beTrue

        val expected = post.service.copy(usage = 0, lastUsageAssessment = now.minusMinutes(10), lastNoUpgradeWarning = None)

        expected must_== tt.service
      }
      "skip no viable upgrade path when already sent this billing cycle" in new accountCleanup {

        val now = new DateTime(DateTimeZone.UTC)

        val usage = 13000001

        mockUsageClient.default = Some(usage)

        // create test account
        val t = ValueAccount(
          newId("u5"),
          newContact(Some("93449")),
          newService(0, now, now.minusMinutes(10), None, ACTIVE, None),
          Some(goodBilling))

        val targetDayOfMonth = now.getDayOfMonth() match {
          case x if x >= 28 => 1
          case x => x + 1
        }

        val tt = t.copy(service = t.service.copy(billingDay = targetDayOfMonth,
          planId = "gold",
          lastUsageWarning = Some(now),
          lastNoUpgradeWarning = Some(now.minusHours(1))))

        createAccount(tt)

        mockNotifications.notifications.clear

        // run assessment
        callAndWaitOnUsageAccounting()

        // check results
        val post = getAccountOrFail(tt.id.email)

        post.service.usage must_== usage
        post.service.planId must_== "gold"
        post.service.lastNoUpgradeWarning must beSome(now.minusHours(1))
        Minutes.minutesBetween(post.service.lastUsageAssessment, now).getMinutes must_== 0

        mockNotifications.notifications.size must_== 1
        mockNotifications.notifications(0).isInstanceOf[AdminNotification] must beTrue

        val expected = post.service.copy(usage = 0, lastUsageAssessment = now.minusMinutes(10))

        expected must_== tt.service
      }
    }
  }
}

trait TestHelpers { self: TestBillingService =>
  val SslHeader = ("ReportGridDecrypter", "")

  def newId(id: String): AccountId = AccountId(id + "@test.net", AccountTokens(id + "-master", id + "-tracking", id + "-production", Some(id + "-development")), "hashOf(" + id + ".password)")

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

  def newService(credit: Int, lastCreditAssessment: DateTime, subsId: Option[String], status: AccountStatus): ServiceInformation = {
    newService(credit, lastCreditAssessment, lastCreditAssessment, subsId, status, None)
  }

  def newService(credit: Int, lastCreditAssessment: DateTime, lastUsageAssessment: DateTime, subsId: Option[String], status: AccountStatus): ServiceInformation = {
    newService(credit, lastCreditAssessment, lastUsageAssessment, subsId, status, None)
  }

  def newService(credit: Int, lastCreditAssessment: DateTime, subsId: Option[String], status: AccountStatus, gracePeriodExpires: Option[DateTime]): ServiceInformation = {
    newService(credit, lastCreditAssessment, lastCreditAssessment, subsId, status, gracePeriodExpires)
  }

  def newService(credit: Int, lastCreditAssessment: DateTime, lastUsageAssessment: DateTime, subsId: Option[String], status: AccountStatus, gracePeriodExpires: Option[DateTime]): ServiceInformation = {
    ServiceInformation(
      "bronze",
      new DateTime(DateTimeZone.UTC),
      credit,
      lastCreditAssessment,
      0,
      lastUsageAssessment,
      None,
      None,
      status,
      gracePeriodExpires,
      2,
      subsId)
  }

  def createAccount(acc: Account): Unit = {
    val f = accounts.trustedCreateAccount(acc)
    while (!f.isDone) {}
  }

  def getAccountOrFail(email: String): Account = {
    val facc = accounts.trustedGetAccount(email)
    while (!facc.isDone) {}
    val acc = facc.toOption.flatMap(_.toOption)
    acc must beSome
    acc.get
  }

  def accountDoesNotExist(email: String) {
    val facc = accounts.trustedGetAccount(email)
    while (!facc.isDone) {}
    val acc = facc.toOption.flatMap(_.toOption)
    acc must beNone
  }

  def callUsageAccounting(sslheader: (String, String) = SslHeader): Future[Option[Unit]] = {
    val f = service.header(sslHeader)
      .query("token", Token.Root.tokenId)
      .contentType[JValue](application / MimeTypes.json)
      .post[JValue]("/accounts/usage/accounting")("")

    f.map { h => h.content.map(_ => Unit) }
  }

  def callAndWaitOnUsageAccounting(): Unit = {
    val res = callUsageAccounting()
    while (!res.isDone) {}
    val v = res.value
    v must eventually(beSome)
    v.get must beSome
  }

  def callCreditAccounting(sslHeader: (String, String) = SslHeader): Future[Option[Unit]] = {
    val f = service.header(sslHeader)
      .query("token", Token.Root.tokenId)
      .contentType[JValue](application / MimeTypes.json)
      .post[JValue]("/accounts/credit/accounting")("")

    f.map { h => h.content.map(_ => Unit) }
  }

  def callAndWaitOnCreditAccounting(): Unit = {
    val res = callCreditAccounting()
    while (!res.isDone) {}
    val v = res.value
    v must eventually(beSome)
    v.get must beSome
  }

  def getTokenFor(email: String, password: String): String = {
    val action = AccountAuthentication(email, password)
    val f: Future[Option[Account]] = postJsonRequest("/accounts/get", action)

    while (!f.isDone) {}

    f.value.get.get.id.tokens.master
  }

  private val verboseInOut = false;

  private val verbosePost = false;

  def postJsonRequest[A, B](url: String, a: A, sslHeader: (String, String) = sslHeader)(implicit d: Decomposer[A], e: Extractor[B]): Future[Option[B]] = {
    if (verboseInOut) {
      println("Url: " + url)
      println("Request: " + Printer.pretty(Printer.render(a.serialize(d))))
    }
    val f = service.header(sslHeader)
      .contentType[JValue](application / MimeTypes.json)
      .post[JValue](url)(a.serialize(d))

    f.map { h =>
      if (verbosePost) {
        println("")
        println(h)
      }
      h.content.map { c =>
        try {
          if (verboseInOut) println("Response: " + Printer.pretty(Printer.render(c)))
          if (verbosePost) {
            println(c)
            println("Deserializing now")
          }
          try {
            if (verbosePost) println("Deserializer: " + e.getClass().getName())
            val result = c.deserialize[B](e)
            if (verbosePost) println("Result: " + result)
            result
          } catch {
            case ex => ex.printStackTrace(System.out); throw ex
          }

        } catch {
          case ex => failure("Error deserializing put request to." + ex.getMessage())
        }
      }
    }
  }

  private val verbosePut = false;

  def putJsonRequest[A, B](url: String, a: A, sslHeader: (String, String) = sslHeader)(implicit d: Decomposer[A], e: Extractor[B]): Future[Option[B]] = {
    if (verboseInOut) {
      println("Url: " + url)
      println("Request: " + Printer.pretty(Printer.render(a.serialize(d))))
    }
    val f = service.header(sslHeader)
      .contentType[JValue](application / MimeTypes.json)
      .put[JValue](url)(a.serialize(d))

    f.map { h =>
      if (verbosePut) {
        println("")
        println(h)
      }
      h.content.map { c =>
        try {
          if (verboseInOut) println("Response: " + Printer.pretty(Printer.render(c)))
          if (verbosePut) {
            println(c)
            println("Deserializing now")
          }
          try {
            if (verbosePut) println("Deserializer: " + e.getClass().getName())
            val result = c.deserialize[B](e)
            if (verbosePut) println("Result: " + result)
            result
          } catch {
            case ex => ex.printStackTrace(System.out); throw ex
          }

        } catch {
          case ex => failure("Error deserializing put request to." + ex.getMessage())
        }
      }
    }
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
        JField("billing", account.billing.serialize(OptionDecomposer(BillingInformation.UnsafeBillingInfoDecomposer)))).filter(fieldHasValue))
  }

  def getAccount[A, B](a: A)(implicit d: Decomposer[A], e: Extractor[B]): Future[Option[B]] = {
    postJsonRequest("/accounts/get", a)(d, e)
  }

  def updateAccountInfo[A, B](a: A)(implicit d: Decomposer[A], e: Extractor[B]): Future[Option[B]] = {
    putJsonRequest("/accounts/info/", a)(d, e)
  }

  def ensureUpdateAccountInfoReturnsError(ua: UpdateAccount, e: String) {
    val res: Future[Option[JValue]] = updateAccountInfo(ua)
    res must whenDelivered { beSomeAndMatch(matchErrorString(e)) }
  }

  def closeAccount[A, B](a: A)(implicit d: Decomposer[A], e: Extractor[B]): Future[Option[B]] = {
    postJsonRequest("/accounts/close", a)(d, e)
  }

  def ensureCloseAccountReturnsError(aa: AccountAuthentication, e: String) {
    val res: Future[Option[JValue]] = closeAccount(aa)
    res must whenDelivered { beSomeAndMatch(matchErrorString(e)) }
  }

  def updateBilling[A, B](a: A)(implicit d: Decomposer[A], e: Extractor[B]): Future[Option[B]] = {
    putJsonRequest("/accounts/billing/", a)(d, e)
  }

  def ensureUpdateBillingReturnsError(ub: UpdateBilling, e: String) {
    val res: Future[Option[JValue]] = updateBilling(ub)(UpdateBilling.UnsafeUpdateBillingDecomposer, implicitly[Extractor[JValue]])
    res must whenDelivered { beSomeAndMatch(matchErrorString(e)) }
  }

  def removeBilling[A, B](a: A)(implicit d: Decomposer[A], e: Extractor[B]): Future[Option[B]] = {
    postJsonRequest("/accounts/billing/delete", a)(d, e)
  }

  def ensureRemoveBillingReturnsError(aa: AccountAuthentication, e: String) {
    val res: Future[Option[JValue]] = removeBilling(aa)
    res must whenDelivered { beSomeAndMatch(matchErrorString(e)) }
  }

  def getAccountInformation[A, B](a: A)(implicit d: Decomposer[A], e: Extractor[B]): Future[Option[B]] = {
    postJsonRequest("/accounts/info/get", a)(d, e)
  }

  def getBillingInformation[A, B](a: A)(implicit d: Decomposer[A], e: Extractor[B]): Future[Option[B]] = {
    postJsonRequest("/accounts/billing/get", a)(d, e)
  }

  def ensureGetAccountReturnsError(aa: AccountAuthentication, e: String): MatchResult[Future[Option[JValue]]] = {
    val res: Future[Option[JValue]] = getAccount(aa)
    res must whenDelivered { beSomeAndMatch(matchErrorString(e)) }
  }

  def ensureGetAccountInformationReturnsError(aa: AccountAuthentication, e: String): MatchResult[Future[Option[JValue]]] = {
    val res: Future[Option[JValue]] = getAccountInformation(aa)
    res must whenDelivered { beSomeAndMatch(matchErrorString(e)) }
  }

  def ensureGetBillingInformationReturnsError(aa: AccountAuthentication, e: String): MatchResult[Future[Option[JValue]]] = {
    val res: Future[Option[JValue]] = getBillingInformation(aa)
    res must whenDelivered { beSomeAndMatch(matchErrorString(e)) }
  }

  def ensureCreateAccountReturnsError(ca: CreateAccount, e: String): MatchResult[Future[Option[JValue]]] = {
    val res = createResult(ca)
    res must whenDelivered { beSomeAndMatch(matchErrorString(e)) }
  }

  def createResult(a: CreateAccount): Future[Option[JValue]] = {
    postJsonRequest("/accounts/", a)(UnsafeCreateAccountDecomposer, implicitly[Extractor[JValue]])
  }

  def create(a: CreateAccount): Account = {
    val f = postJsonRequest("/accounts/", a)(UnsafeCreateAccountDecomposer, implicitly[Extractor[Account]])
    while (!f.isDone) {}
    val value = f.value
    value must eventually(beSome)
    val option = value.get
    option must beSome
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
    val db = mongo.database(testDatabase)
    val q = remove.from(testCollection)
    db(q)
  }

  def createTestAccounts() {
    create(createAccount1)
    create(createAccount2)
  }

  def testFutureOptionError(f: => Future[Option[JValue]], e: String): Unit = {
    while (!f.isDone) {}
    f.value must eventually(beSome)
    val con = f.value.get
    con must beSome
    con.get must matchErrorString(e)
  }

  def accountCount(): Int = {
    val accounts = accountsFactory(null)
    val f = accounts.getAll
    while (!f.isDone) {}
    f.value.toList.flatMap(_.flatMap(_.toOption)).size
  }

  def accountsUnchanged() {
    accountCount() must beEqualTo(3)

    createAccounts.foreach(acc => {
      val checkAcc = getAccountOrFail(acc.email)
      checkAcc must matchAccount(acc)
    })
  }

  def createAccountSetup() {
    cleanup
    createAccounts.foreach(create)
  }

  def createAccountTeardown() {
    cleanup
  }

  def trustedAccountCreation(acc: Account): Unit = {
    val accounts = accountsFactory(null)
    accounts.trustedCreateAccount(acc)
  }

  case class matchErrorString(s: String) extends Matcher[JValue]() {
    def apply[T <: JValue](e: Expectable[T]): MatchResult[T] = {
      val j = e.value
      j match {
        case JString(a: String) => result(s.equals(a), "Error strings are the same.", "Error strings not the same: [" + s + "] vs [" + a + "]", e);
        case x => result(false, "not applicable", "Expected JString but was: " + x, e)
      }
    }
  }

  case class sameTime(d1: DateTime) extends Matcher[DateTime]() {
    def apply[T <: DateTime](e: Expectable[T]): MatchResult[T] = {
      val d2 = e.value
      result(d1.equals(d2), "Dates are equal", "DateTimes not equal " + d1.toString() + " vs " + d2.toString(), e)
    }
  }

  case class matchBilling(b1: Option[BillingInformation]) extends Matcher[Option[BillingInformation]] {
    def apply[T <: Option[BillingInformation]](e: Expectable[T]): MatchResult[T] = {
      val b2 = e.value
      if (b1.isDefined && b2.isDefined) {
        val v1 = b1.get
        val v2 = b2.get

        val test = v1.cardholder == v2.cardholder &&
          v1.number.endsWith(v2.number) &&
          v1.expDate == v2.expDate

        result(test, "Billing information is the same", "Billing info differs: " + v1 + " \n vs\n" + v2, e)
      } else if (b1.isEmpty && b2.isEmpty) {
        result(true, "Both are None", "na", e)
      } else {
        result(false, "na", "One billing info is defined the other is not.", e)
      }
    }
  }

  case class matchId(a1: AccountId) extends Matcher[AccountId] {
    def apply[T <: AccountId](e: Expectable[T]): MatchResult[T] = {
      val a2 = e.value
      val test = a1.email == a2.email &&
        a1.tokens == a2.tokens

      result(test, "Ids are compatible", "Ids differ: " + a1 + "\n vs\n" + a2, e)
    }
  }

  case class sameTimeOption(d1: Option[DateTime]) extends Matcher[Option[DateTime]]() {
    def apply[T <: Option[DateTime]](e: Expectable[T]): MatchResult[T] = {
      val d2 = e.value
      if (d1.isDefined && d2.isDefined) {
        result(d1.get must sameTime(d2.get), e)
      } else if (d1.isEmpty && d2.isEmpty) {
        result(true, "Both are None", "na", e)
      } else {
        result(false, "na", "One date is defined the other is not.", e)
      }
    }
  }

  case class matchAccount(ca: CreateAccount) extends Matcher[Account]() {
    def apply[T <: Account](e: Expectable[T]): MatchResult[T] = {

      val v = e.value

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

      result(signup && billing, "Account creation info matches the given account.", "Error account creation info doesn't match the given account.", e)
    }
  }

  case class matchAccountInfo(ca: CreateAccount) extends Matcher[AccountInformation]() {
    def apply[T <: AccountInformation](e: Expectable[T]): MatchResult[T] = {

      val ai = e.value

      val signup = ca.email == ai.id.email &&
        ca.planId == ai.service.planId &&
        ca.contact == ai.contact

      result(signup, "Account creation info matches the given account.", "Error account creation info doesn't match the given account.", e)
    }
  }

  case class matchBillingInfo(ca: CreateAccount) extends Matcher[BillingInformation]() {
    def apply[T <: BillingInformation](e: Expectable[T]): MatchResult[T] = {

      val cabo = ca.billing
      val bi = e.value

      val billing = if (cabo.isDefined) {
        val cab = cabo.get

        cab.cardholder == bi.cardholder &&
          cab.expMonth == bi.expMonth &&
          cab.expYear == bi.expYear &&
          cab.billingPostalCode == bi.billingPostalCode
      } else {
        cabo.isEmpty
      }

      result(billing, "Account creation info matches the given account.", "Error account creation info doesn't match the given account.", e)
    }
  }

  case class beDisabled() extends Matcher[Account] {
    def apply[T <: Account](e: Expectable[T]): MatchResult[T] = {
      val a: T = e.value
      result(a.service.status == AccountStatus.DISABLED, "Account is disabled as expected.", "Account was not disabled as expected [" + a.service.status + "].", e)
    }
  }

  case class beSomeAndMatch[A](delegateMatcher: Matcher[A]) extends Matcher[Option[A]] {
    def apply[T <: Option[A]](e: Expectable[T]): MatchResult[T] = {
      e must beSome
      result(e.value.get must delegateMatcher, e)
    }
  }
}
