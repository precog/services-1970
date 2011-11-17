package com.reportgrid.billing.braintree

import com.braintreegateway._

import blueeyes.concurrent.Future

import scalaz._
import scala.collection.JavaConverters._

import com.reportgrid.billing.BillingInformationStore
import com.reportgrid.billing.BillingData
import com.reportgrid.billing.BillingInformation
import com.reportgrid.billing.ContactInformation
import com.reportgrid.billing.CreateAccount

class BraintreeService(gateway: BraintreeGateway, environment: Environment) extends BillingInformationStore {

  def create(token: String, email: String, contact: ContactInformation, info: BillingInformation): FV[String, BillingInformation] = {
    Future.sync(if (conflictingCustomers(token, email)) {
      Failure("The account token or email requested is already in use.")
    } else {
      val request = new CustomerRequest()
          .id(token)
          .email(email)
          .company(contact.company)
          .website(contact.website)
          .creditCard()
            .cardholderName(info.cardholder)
            .number(info.number)
            .expirationDate(info.expDate)
            .cvv(info.cvv)
            .billingAddress()
              .postalCode(info.billingPostalCode)
          .done()
        .done();

      val result: Result[Customer] = gateway.customer().create(request)
      if (result.isSuccess()) {
        Success(info)
      } else {
        val ccv = result.getCreditCardVerification()
        val occv = if(ccv == null) None else Some(ccv)
        Failure(collapseErrors(result.getErrors(), occv))
      }
    })
  }
  
  def getByEmail(email: String): FV[String, BillingInformation] = {
    val customers = findCustomersWithEmail(email).toList
    Future.sync(customers match {
      case c :: Nil => toBillingData(c).info.map(bi => Success(bi)).getOrElse(Failure("Unable to find the specified customer."))
      case c :: cs  => Failure("Multiple customers found with that email.")
      case Nil      => Failure("No customers found with that email.")
    })
  }
  
  def getByToken(token: String): FV[String, BillingInformation] = {
    try {
      val customer = gateway.customer().find(token)
      val billingData = toBillingData(customer).info
      Future.sync(billingData.map(bi => Success(bi)).getOrElse(Failure("Unable to find the specified customer.")))
    } catch {
      case ex => Future.sync(Failure("Unable to find the specified customer."))
    }
  }
  
  private def toBillingData(cust: Customer): BillingData = {
    val cards = cust.getCreditCards()
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
    BillingData(billingInfo, subs)

  }
  
  def update(token: String, email: String, contact: ContactInformation, info: BillingInformation): FV[String, BillingInformation] = {
    val cust = findCustomer(token)
    
    cust.map[FV[String, BillingInformation]] { c => {
      val cards = c.getCreditCards()
      if(cards.size == 0) {
        val request = new CustomerRequest()
            .email(email)
            .company(contact.company)
            .website(contact.website)
            .creditCard()
              .cardholderName(info.cardholder)
              .number(info.number)
              .expirationDate(info.expDate)
              .cvv(info.cvv)
              .billingAddress()
                .postalCode(info.billingPostalCode)
            .done()
          .done();
  
        val result: Result[Customer] = gateway.customer().update(c.getId(), request)
        
        Future.sync(if (result.isSuccess()) {
          Success(info)
        } else {
          val ccv = result.getCreditCardVerification()
          val occv = if(ccv == null) None else Some(ccv)
          Failure(collapseErrors(result.getErrors(), occv))
        })   
      } else if(cards.size == 1) {
        val ccToken = cards.get(0).getToken()

        val request = new CustomerRequest()
            .email(email)
            .company(contact.company)
            .website(contact.website)
            .creditCard()
              .cardholderName(info.cardholder)
              .options().updateExistingToken(ccToken).done()
              .number(info.number)
              .expirationDate(info.expDate)
              .cvv(info.cvv)
              .billingAddress()
                .postalCode(info.billingPostalCode)
                .options().updateExisting(true).done()
            .done()
          .done();
  
        val result: Result[Customer] = gateway.customer().update(c.getId(), request)
        
        Future.sync(if (result.isSuccess()) {
          Success(info)
        } else {
          val ccv = result.getCreditCardVerification()
          val occv = if(ccv == null) None else Some(ccv)
          Failure(collapseErrors(result.getErrors(), occv))
        })           
      } else {
        Future.sync(Failure("Unable to update the credit card information."))
      }
    }}.getOrElse {
      create(token, email, contact, info)      
    }
  }
  
  def removeByEmail(email: String): FV[String, BillingInformation] = {
    val customers = findCustomersWithEmail(email).toList
    Future.sync(customers match {
      case c :: Nil => {
        gateway.customer().delete(c.getId())
        toBillingData(c).info.map(bi => Success(bi)).getOrElse(Failure("Unable to find the specified customer."))
      }
      case c :: cs  => Failure("Multiple customers found with that email.")
      case Nil      => Failure("No customers found with that email.")
    })
  }

  def removeByToken(token: String): FV[String, BillingInformation] = {
    Future.sync(findCustomer(token) match {
      case Some(c) => {
        gateway.customer().delete(c.getId())
        toBillingData(c).info.map(bi => Success(bi)).getOrElse(Failure("Unable to find the specified customer."))
      }
      case None => Failure("Unable to locate customer for token: " + token)
    })
  }
  
  def startSubscription(token: String, planId: String): FV[String, String] = {
    try {
      val customer = gateway.customer().find(token)
      val subscription = newSubscription(customer, planId)
      Future.sync(subscription.map(_.getId()))
    } catch {
      case ex => Future.sync(Failure("Unable to find the specified customer."))
    }
  }

  def changeSubscriptionPlan(subscriptionId: String, planId: String): FV[String, String] = {
    val plans = gateway.plan().all()
    val matchingPlans = plans.asScala.filter(p => p.getId() == planId)
    Future.sync(if (matchingPlans.size == 1) {
      val plan = matchingPlans(0)
      val request = new SubscriptionRequest()
                          .planId(plan.getId)

      val result: Result[Subscription] = gateway.subscription().update(subscriptionId, request)
      if (result.isSuccess) {
        Success(result.getTarget().getId())
      } else {
        Failure(collapseErrors(result.getErrors(), None))
      }
    } else {
      Failure("Invalid plan selection.")
    })
  }

  def stopSubscriptionByToken(token: String): FV[String, String] = error("billing.stopSubscriptionByToken Not yet implemented.")
  def stopSubscriptionBySubscriptionId(subscriptionId: String): FV[String, String] = error("billing.stopSubscriptionById Not yet implemented.")
  
  // Customer management
  
  def newCustomer(create: CreateAccount, billing: BillingInformation, accountToken: String): Validation[String, Customer] = {
    if (conflictingCustomers(accountToken, create.email)) {
      Failure("The account token or email requested is already in use.")
    } else {
      val request = new CustomerRequest()
          .id(accountToken)
          .email(create.email)
          .company(create.contact.company)
          .website(create.contact.website)
          .creditCard()
            .cardholderName(billing.cardholder)
            .number(billing.number)
            .expirationDate(billing.expDate)
            .cvv(billing.cvv)
            .billingAddress()
              .postalCode(billing.billingPostalCode)
          .done()
        .done();

      val result: Result[Customer] = gateway.customer().create(request)
      if (result.isSuccess()) {
        Success(result.getTarget())
      } else {
        val ccv = result.getCreditCardVerification()
        val occv = if(ccv == null) None else Some(ccv)
        Failure(collapseErrors(result.getErrors(), occv))
      }
    }
  }

  def findAllCustomers(): Iterator[Customer] = {
    gateway.customer().all().asScala.iterator
  }

  def findCustomer(token: String): Option[Customer] = {
    try {
      val customer = gateway.customer().find(token)
      Some(customer)
    } catch {
      case ex => None
    }
  }

  def findCustomersWithEmail(email: String): Iterator[Customer] = {
    val request = new CustomerSearchRequest().email().is(email)
    val customers = gateway.customer().search(request)
    customers.asScala.iterator
  }

  def removeCustomer(token: String): Validation[String, Unit] = {
    findCustomer(token) match {
      case Some(c) => {
        gateway.customer().delete(c.getId())
        Success(Unit)
      }
      case None => Failure("Unable to locate customer for token: " + token)
    }
  }

  def conflictingCustomers(token: String, email: String): Boolean = {
    def found(itr: Iterator[Customer]): Boolean = itr.hasNext

    findCustomer(token).isDefined || found(findCustomersWithEmail(email))
  }

  def deleteAllCustomers(): Unit = {
    if (environment != Environment.SANDBOX) throw new IllegalArgumentException("Delete all only available in sandbox environment.")
    gateway.getConfiguration()
    val ids = findAllCustomers.map(_.getId)
    ids.foreach(id => removeCustomer(id))
  }


  // Plan management

  def findPlans(): List[Plan] = {
    gateway.plan().all().asScala.toList
  }

  def findPlan(planId: String): Option[Plan] = {
    val plan = findPlans().filter(p => p.getId() == planId)
    plan match {
      case Nil => None
      case p :: Nil => Some(p)
      case _ => None // This should never happen???
    }
  }

  // Subscription management
  
  def findActiveSubscription(accountToken: String): Validation[String, Subscription] = {

    def getCreditCard(c: Customer): Option[CreditCard] = {
      val cards = c.getCreditCards()
      if (cards.size == 1) {
        Some(cards.get(0))
      } else {
        None
      }
    }

    def getSubscription(cc: CreditCard): Option[Subscription] = {
      val subs = cc.getSubscriptions()
      if (subs.size == 1) {
        Some(subs.get(0))
      } else {
        None
      }
    }

    val customer: Option[Customer] = findCustomer(accountToken)

    customer.map[Validation[String, Subscription]] { c =>
      val card = getCreditCard(c)
      card.map[Validation[String, Subscription]] { cc =>
        val subs = getSubscription(cc)
        subs.map[Validation[String, Subscription]] { s =>
          Success(s)
        } getOrElse {
          Failure("Unable to find active subscription for: " + accountToken)
        }
      } getOrElse {
        Failure("Unable to find card on file for account: " + accountToken)
      }
    } getOrElse {
      Failure("Unable to find a customer for the token: " + accountToken)
    }
  }

  def hasActiveSubscriptions(accountToken: String): Boolean = {
    findActiveSubscription(accountToken).isSuccess
  }

  def newSubscription(customer: Customer, planId: String): Validation[String, Subscription] = {
    if (hasActiveSubscriptions(customer.getId())) {
      Failure("This customer already has an active subscription.")
    } else {
      val plans = gateway.plan().all()
      val matchingPlans = plans.asScala.filter(p => p.getId() == planId)
      if (matchingPlans.size == 1) {
        val plan = matchingPlans(0)
        val paymentToken = customer.getCreditCards().get(0).getToken()
        val request = new SubscriptionRequest()
          .paymentMethodToken(paymentToken)
          .planId(plan.getId)

        val result: Result[Subscription] = gateway.subscription().create(request)
        if (result.isSuccess) {
          Success(result.getTarget())
        } else {
          Failure(collapseErrors(result.getErrors(), None))
        }
      } else {
        Failure("Plan not found (" + planId + ")")
      }
    }
  }

  def stopSubscription(subscriptionId: Option[String]): Validation[String, Unit] = {
    if (subscriptionId.isDefined) {
      val result = gateway.subscription().cancel(subscriptionId.get)
      if (result.isSuccess) {
        Success(Unit)
      } else {
        Failure(collapseErrors(result.getErrors(), None))
      }
    } else {
      Success(Unit)
    }
  }

  // Helper methods
  
  private def collapseErrors(errors: ValidationErrors, verification: Option[CreditCardVerification]): String = {
    val validationErrors = errors.getAllDeepValidationErrors().asScala.foldLeft("")((acc, a) => acc + " " + a.getMessage())

    val verificationError = verification.map(ccv => {
      val majorCode = ccv.getProcessorResponseCode().substring(0,1)
      majorCode match {
        case "1" => ""
        case "2" => " Credit card declined. Reason [" + ccv.getProcessorResponseText() + "]"
        case "3" => " Error with credit card processing service. Reason [" + ccv.getProcessorResponseText() + "]"
        case _   => " Error will billing service. Reason [Credit card processing gateway produced unexepcted error.]"
      }
    }).getOrElse("")
    
    "Billing errors:" + validationErrors + verificationError 
  }
}