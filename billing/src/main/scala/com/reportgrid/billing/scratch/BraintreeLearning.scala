package com.reportgrid.billing.scratch

import com.reportgrid.billing._
import com.reportgrid.billing.BillingServiceHandlers._
import com.braintreegateway._
import com.reportgrid.billing.braintree._
import scalaz._

object BraintreeServiceExamples {
  def main(args: Array[String]) = {
    val gateway = new BraintreeGateway(
      Environment.SANDBOX,
      "zrfxsyvnfvf8x73f",
      "pyvkc5m7g6bvztfv",
      "zxv9467dx36zkd8y");
    val service = new BraintreeService(gateway, Environment.SANDBOX)
    happyExamples(service)
    errorExamples(service)
  }

  def happyExamples(service: BraintreeService) = {

    val billing = BillingInformation(
      "John Doe",
      "4111111111111111",
      5,
      2012,
      "123",
      "60607")

      
    val signup = Signup("john@doe.com", 
          Some("abc123"), 
          Some("abc.com"), 
          "starter", 
          None,
          "abc123") 

    val account = TestAccounts.testAccount

    val result: Validation[String, Customer] = service.newUserAndCard(signup, billing, "token")

    result match {
      case Success(c) => println("Success: " + c.getCreditCards().get(0).getToken + " -> " + c.getId)
      case Failure(e) => println("Error: " + e)
    }

    result match {
      case Success(c) => {
        val subs: Validation[String, Subscription] = service.newSubscription(c, "bronze")

        subs match {
          case Success(s) => println("Subscription succeeded")
          case Failure(e) => println("Subscription failed: " + e)
        }

        val subs2: Validation[String, Subscription] = service.newSubscription(c, "bronze")

        subs2 match {
          case Success(s) => println("Subscription succeeded")
          case Failure(e) => println("Subscription failed: " + e)
        }

      }
      case _ => ()
    }
  }

  def errorExamples(service: BraintreeService) = {

  }

}

