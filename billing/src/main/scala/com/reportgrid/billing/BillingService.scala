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

  def accountsFactory(config: ConfigMap): PublicAccounts
  def usageClientFactory(config: ConfigMap): UsageClient
  def notificationsFactory(config: ConfigMap): NotificationSender

  val billing = service("billing", "1.1.2") {
    healthMonitor { monitor =>
      context =>
        startup {
          val config = context.config

          val accounts = accountsFactory(config)
          val notifications = notificationsFactory(config)

          val bc = BillingConfiguration(accounts, notifications)
          Future.sync(bc)
        } -> request { config =>
          headerParameterRequired("ReportGridDecrypter", "Service may only be accessed via SSL.") {
            jsonp {
              path("/accounts/") {
                post { new CreateAccountHandler(config, monitor) } ~
                path("close") {
                  post {
                    new CloseAccountHandler(config, monitor)
                  }
                } ~
                path("get") {
                  post { 
                    new LegacyGetAccountHandler(config, monitor) 
                  }
                } ~
                path("billing/") {
                  put {
                    new UpdateBillingHandler(config, monitor)
                  } ~
                  path("get") {
                    post {
                      new GetBillingHandler(config, monitor)
                    }
                  } ~
                  path("delete") {
                    post {
                      new RemoveBillingHandler(config, monitor)
                    }
                  }
                } ~
                path("info/") {
                  put {
                    new UpdateAccountHandler(config, monitor)
                  } ~
                  path("get") {
                    post {
                      new GetAccountHandler(config, monitor)
                    }
                  } 
                } ~
                path("credit/accounting") {
                  post { 
                    new CreditAccountingHandler(config, monitor) 
                  }
                } ~
                path("usage/accounting") {
                  post { 
                    new UsageAccountingHandler(config, monitor) 
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
