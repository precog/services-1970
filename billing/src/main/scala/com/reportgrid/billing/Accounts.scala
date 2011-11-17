package com.reportgrid.billing

import scala.collection.JavaConverters._

import scalaz._
import scalaz.Scalaz._

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.DateMidnight
import org.joda.time.Days

import com.braintreegateway._
import com.reportgrid.billing.braintree.BraintreeService

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
      case (Success(a), Success(b)) => Success((a,b))
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
}

class PrivateAccounts(config: ConfigMap, accountStore: AccountInformationStore, billingStore: BillingInformationStore, tokens: TokenGenerator) extends PublicAccounts {

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
    val today = created.toDateMidnight().toDateTime()
    val aid = AccountId(create.email, tokens, PasswordHash.saltedHash(create.password))
    val srv = ServiceInformation(
      create.planId,
      created,
      credit,
      today,
      0,
      AccountStatus.ACTIVE,
      None,
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
      if(hasCredit) {
        Future.sync(Success(BillingData(None, None)))  
      } else {
        Future.sync(Failure("Unable to create account without account credit or billing information."))
      }
    )
  }

  def buildAccount(billingData: BillingData, accountInfo: AccountInformation): Account = {
    val service = accountInfo.service
    val newService = ServiceInformation(
      service.planId,
      service.accountCreated,
      service.credit,
      service.lastCreditAssessment,
      service.usage,
      service.status,
      service.gracePeriodExpires,
      billingData.subscriptionId)

    ValueAccount(
      accountInfo.id,
      accountInfo.contact,
      newService,
      billingData.info)
  }

  def undoBilling(token: String): Unit = {
    println("Rolling back billing")
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
    
    tokens.map(_.map{ t => {
      AccountTokens(t._1._1._1, t._1._1._2, t._1._2, Some(t._2))
    }})
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
          val nai = if(bd.subscriptionId.isDefined) {
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
    // Cases that need to be handled here
    // - planId changed
    // - credit changed to zero? (Not currently supported.)
    accountUpdateInfo.service.subscriptionId.map[FV[String, BillingData]] { s =>
      if(currentAccount.service.planId == accountUpdateInfo.service.planId) {
        Future.sync(Success(BillingData(currentAccount.billing, Some(s))))        
      } else {
        val result = billingStore.changeSubscriptionPlan(s, accountUpdateInfo.service.planId)
        result.map(_.map(ns => BillingData(currentAccount.billing, Some(ns))))
      }
    }.getOrElse {
      Future.sync(Success(BillingData(currentAccount.billing, None)))                
    }
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
      fvApply(getAccountInfo(auth), authorizeAccountAccess(auth, _: AccountInformation)),
      combineWithBillingInfo(auth, _: AccountInformation))
  }

  private def getAccountInfo(auth: AccountAuthentication): FV[String, AccountInformation] = {
    accountStore.getByEmail(auth.email)
  }

  private def authorizeAccountAccess(auth: AccountAuthentication, accountInfo: AccountInformation): FV[String, AccountInformation] = {
    if (PasswordHash.checkSaltedHash(auth.password, accountInfo.id.passwordHash)) {
      Future.sync(Success(accountInfo))
    } else {
      Future.sync(Failure("You must provide a valid email or account token and a valid password."))
    }
  }

  private def combineWithBillingInfo(auth: AccountAuthentication, accountInfo: AccountInformation): FV[String, Account] = {
    if (PasswordHash.checkSaltedHash(auth.password, accountInfo.id.passwordHash)) {
      fvApply(getBillingInfo(auth), (billing: Option[BillingInformation]) => {
        Future.sync(Success(
          ValueAccount(
            accountInfo.id,
            accountInfo.contact,
            accountInfo.service,
            billing)))
      })
    } else {
      Future.sync(Failure("You must provide a valid email or account token and a valid password."))
    }
  }

  private def getBillingInfo(auth: AccountAuthentication): FV[String, Option[BillingInformation]] = {
    billingStore.getByEmail(auth.email).map(v => Success(v.toOption))
  }
}

trait AccountInformationStore {
  type FV[E, A] = Future[Validation[E, A]]

  def create(info: AccountInformation): FV[String, AccountInformation]

  def getByEmail(email: String): FV[String, AccountInformation]
  def getByToken(token: String): FV[String, AccountInformation]

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

  def update(newInfo: AccountInformation): FV[String, AccountInformation] = {
    fvApply(getByToken(newInfo.id.tokens.master), (current: AccountInformation) => {
      val updatedAccountInfo = validateAccountUpdates(current, newInfo)
      updatedAccountInfo match {
        case Success(ai) => internalUpdate(ai)
        case Failure(e)  => Future.sync(Failure(e))
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

  def removeByEmail(email: String): FV[String, AccountInformation] = error("accountStore.removeByEmail Not yet implemented.")
  def removeByToken(token: String): FV[String, AccountInformation] = error("accountStore.removeByToken Not yet implemented.")
     
}

trait BillingInformationStore {
  type FV[E, A] = Future[Validation[E, A]]

  def create(token: String, email: String, contact: ContactInformation, info: BillingInformation): FV[String, BillingInformation]

  def getByEmail(email: String): FV[String, BillingInformation]
  def getByToken(token: String): FV[String, BillingInformation]

  def update(token: String, email: String, contact: ContactInformation, info: BillingInformation): FV[String, BillingInformation]

  def removeByEmail(email: String): FV[String, BillingInformation]
  def removeByToken(token: String): FV[String, BillingInformation]

  def startSubscription(token: String, planId: String): FV[String, String]
  
  def changeSubscriptionPlan(subscriptionId: String, planId: String): FV[String, String]
  
  def stopSubscriptionByToken(token: String): FV[String, String]
  def stopSubscriptionBySubscriptionId(subscriptionId: String): FV[String, String]

  def findPlans(): List[Plan]
  def findPlan(planId: String): Option[Plan]
}

class Accounts(config: ConfigMap, tokens: TokenGenerator, billingService: BraintreeService, database: Database, accountsCollection: String) {
  
  import FutureValidationHelpers._
  
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
            val tokens = newAccountTokens(toPath(create.email))

            tokens.flatMap[Validation[String, Account]] {
              case Success(ats) => {
                val billing = establishBilling(create, ats.master, credit > 0)

                billing match {
                  case Success(ob) => {
                    val tracking = createTrackingAccount(create, ob, ats, credit)
                    tracking.map {
                      case Success(ta) => {
                        Success(buildAccount(ob, ta))
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
  }
  
  def undoNewTokens(accountTokens: AccountTokens): Unit = {
    accountTokens.development.foreach(tokens.deleteToken(_))
    tokens.deleteToken(accountTokens.production)
    tokens.deleteToken(accountTokens.tracking)
    tokens.deleteToken(accountTokens.master)
  }
  
  private def find(email: Option[String], token: Option[String]): FV[String, Account] = {
    token.map(findByToken).getOrElse {
      email.map(findByEmail(_)).getOrElse {
        Future.sync(Failure("You must provide either an email or token in order to update your account."))
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
    validateOther(create).orElse(validateBilling(create.billing))
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
    val plans = billingService.findPlans()
    plans.any(plan => plan.getId() == p)
  }

  def updateAccount(a: Account): FV[String, Account] = Future.sync(Failure("Not yet implemented"))

  def cancelWithEmail(e: String, p: String): FV[String, Account] = Future.sync(Failure("Not yet implemented"))
  def cancelWithToken(t: String, p: String): FV[String, Account] = Future.sync(Failure("Not yet implemented"))

  def findByToken(t: String): FV[String, Account] = {
    val query = select().from(accountsCollection).where(("id.tokens.master" === t))
    findByQuery(query)
  }

  def findByEmail(e: String, forceNotFound: Boolean = false): FV[String, Account] = {
    if (forceNotFound) Future.sync(Failure("Account not found.")) else {
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
        trustedUpdateAccount(acc.id.tokens.master, subscriptionAdjusted)
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
      val cust = billingService.findCustomer(acc.id.tokens.master)
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
        billing.billing)

      val newba = billingService.newCustomer(c, b, acc.id.tokens.master)
      val subs = billing.subscriptionId.flatMap { s =>
        val c = billingService.findCustomer(acc.id.tokens.master)
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

  private def trustedUpdateAccount(acc: Account): Future[Validation[String, Account]] = {
    trustedUpdateAccount(acc.id.tokens.master, acc)
  }

  private def trustedUpdateAccount(token: String, acc: Account): Future[Validation[String, Account]] = {
    updateTracking(token, acc.asTrackingAccount).map(_.map(_ => acc))
    // clearly missing billing update here
  }

  private def updateTracking(t: String, a: TrackingAccount): Future[Validation[String, Unit]] = {
    val jval: JValue = a.serialize
    val q = MongoQueryBuilder.update(accountsCollection).set(jval --> classOf[JObject]).where("id.tokens.master" === t)
    val res: Future[Unit] = database(q)
    res.map { u =>
      Success(u)
    }
  }

  def closeAccount(account: Account): Future[Validation[String, String]] = {
    Future.sync(Success("Account closed."))
  }

  def newAccountTokens(path: String): FV[String, AccountTokens] = {
    val master = newToken(path)
    val tokens: FV[String, (((String, String), String), String)] = fvApply(newToken(path), (m: String) => {
      val prod = newChildToken(m, "/prod/")
      val tracking: FV[String, String] = Future.sync(Success("tracking-tbd"))
      val dev = newChildToken(m, "/dev/")
      fvJoin(fvJoin(fvJoin(master, tracking), prod), dev)
    });
    
    tokens.map(_.map{ t => {
      AccountTokens(t._1._1._1, t._1._1._2, t._1._2, Some(t._2))
    }})
  }
  
  def newChildToken(parent: String, path: String): FV[String, String] = {
    tokens.newChildToken(parent, path)
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

  def createTrackingAccount(create: CreateAccount, billing: Option[BillingAccount], tokens: AccountTokens, credit: Int): Future[Validation[String, TrackingAccount]] = {
    val created = new DateTime(DateTimeZone.UTC)
    val today = created.toDateMidnight().toDateTime()
    val aid = AccountId(create.email, tokens, PasswordHash.saltedHash(create.password))
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
    val cust = billingService.findCustomer(tracking.id.tokens.master)
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
          c.getBillingAddress().getPostalCode(),
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

//case class UpdateAccount(
//  auth: AccountAuthentication,
//  newEmail: Option[String],
//  newPassword: Option[Password],
//  planId: String,
//  contact: ContactInformation)
//
//trait UpdateAccountSerialization {
//
//  implicit val UpdateAccountDecomposer: Decomposer[UpdateAccount] = new Decomposer[UpdateAccount] {
//    override def decompose(update: UpdateAccount): JValue = JObject(
//      List(
//        JField("authentication", update.auth.serialize),
//        JField("newEmail", update.newEmail.serialize),
//        JField("newPassword", update.newPassword.serialize),
//        JField("planId", update.planId.serialize),
//        JField("contact", update.contact.serialize)).filter(fieldHasValue))
//  }
//
//  
//  implicit val UpdateAccountExtractor: Extractor[UpdateAccount] = new Extractor[UpdateAccount] with ValidatedExtraction[UpdateAccount] {
//    override def validated(obj: JValue): Validation[Error, UpdateAccount] = (
//      (obj \ "authentication").validated[AccountAuthentication] |@|
//      (obj \ "newEmail").validated[Option[String]] |@|
//      (obj \ "newPassword").validated[Option[Password]] |@|
//      (obj \ "planId").validated[String] |@|
//      (obj \ "contact").validated[ContactInformation]).apply(UpdateAccount(_, _, _, _, _))
//  }
//}
//
//object UpdateAccount extends UpdateAccountSerialization


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

class AuditResults {
}

class AssessmentResults {
}

object TestAccounts {
  val testAccount = new ValueAccount(
    AccountId("john@doe.com", AccountTokens("master", "tracking", "prod", Some("dev")), "hash"),
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
