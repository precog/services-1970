package com.reportgrid.billing

import blueeyes.json.JsonAST._
import blueeyes.{ BlueEyesServer, BlueEyesServiceBuilder }
import blueeyes.concurrent.Future
import blueeyes.persistence.mongo.Mongo
import blueeyes.persistence.mongo.mock.MockMongo
import blueeyes.core.http.combinators.HttpRequestCombinators
import blueeyes.core.http.MimeTypes._
import blueeyes.core.http.{ HttpRequest, HttpResponse }
import blueeyes.core.service.HttpClient
import blueeyes.core.service.ServerHealthMonitorService
import blueeyes.core.data.{ BijectionsChunkJson, BijectionsChunkFutureJson, ByteChunk, BijectionsChunkString }
import blueeyes.core.service._

import com.reportgrid.billing.BillingServiceHandlers._
import com.reportgrid.billing._
import com.reportgrid.billing.braintree._

import com.braintreegateway.{ BraintreeGateway, Environment }

import net.lag.configgy.ConfigMap

trait BillingService extends BlueEyesServiceBuilder with BijectionsChunkString with BijectionsChunkJson with BijectionsChunkFutureJson {

  implicit def httpClient: HttpClient[ByteChunk]

  def accountsFactory(config: ConfigMap): Accounts
  def mailerFactory(config: ConfigMap): Mailer

  val billing = service("billing", "1.0.0") {
    healthMonitor { monitor =>
      serviceLocator { locator =>
        context =>
          startup {
            val config = context.config

            val accounts = accountsFactory(config)
            val mailer = mailerFactory(config)

            val bc = BillingConfiguration(accounts, mailer)
            Future.sync(bc)
          } -> request { config =>
            headerParameterRequired("ReportGridDecrypter", "Service may only be accessed via SSL.") {
              jsonp {
                path("/accounts/") {
                  put {
                    monitor.count(".accounts.create.requests")
                    monitor.time(".accounts.create.timing") {
                      new CreateAccountHandler(config, monitor) 
                    }
                  } ~
//                  delete { new CloseAccountHandler(config, monitor) } ~
//                  post { new UpdateAccountHandler(config, monitor) } ~
                  path("get") {
                    post { 
                      monitor.count(".accounts.get.requests")
                      monitor.time(".accounts.get.timing") {
                        new GetAccountHandler(config, monitor) 
                      }
                    }
                  } ~
//                  path("usage") {
//                      put { new AccountUsageHandler(config, monitor) }
//                  } ~
//                  path("audit") {
//                      post { new AccountAuditHandler(config, monitor) }              
//                  } ~
                  path("assess") {
                    post { 
                      monitor.count(".accounts.assessment.requests")
                      monitor.time(".accounts.assessment.timing") {
                        new AccountAssessmentHandler(config, monitor) 
                      }
                    }
                  }
                }
              }
            }
          } -> shutdown { config =>
            config.shutdown
            ().future
          }
      }
    }
  }
}
