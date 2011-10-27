package com.reportgrid.billing.braintree

import com.braintreegateway._

import scalaz._
import scala.collection.JavaConverters._

import com.reportgrid.billing.BillingInformation
import com.reportgrid.billing.CreateAccount

class BraintreeService(gateway: BraintreeGateway, environment: Environment) {

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
            .postalCode(create.contact.address.postalCode)
          .done()
        .done();

      val result: Result[Customer] = gateway.customer().create(request)
      if (result.isSuccess()) {
        Success(result.getTarget())
      } else {
        Failure(collapseErrors(result.getErrors()))
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
          Failure(collapseErrors(result.getErrors()))
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
        Failure(collapseErrors(result.getErrors()))
      }
    } else {
      Success(Unit)
    }
  }

  // Helper methods
  
  private def collapseErrors(errors: ValidationErrors): String = {
    errors.getAllDeepValidationErrors().asScala.foldLeft("Billing errors:")((acc, a) => acc + " " + a.getMessage())
  }
}