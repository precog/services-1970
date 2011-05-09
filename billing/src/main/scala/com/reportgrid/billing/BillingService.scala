package com.reportgrid.billing

import blueeyes.BlueEyesServiceBuilder
import blueeyes.core.data.{BijectionsChunkReaderString, BijectionsChunkReaderJson}

/**
 * Created by IntelliJ IDEA.
 * User: knuttycombe
 * Date: 5/9/11
 * Time: 10:53 AM
 * To change this template use File | Settings | File Templates.
 */

trait BillingService extends BlueEyesServiceBuilder with BijectionsChunkReaderJson with BijectionsChunkReaderString {
  val billingService = service("analytics", "0.01") {
    healthMonitor { monitor => context =>
      startup {
        import context._

        val mongoConfig = config.configMap("mongo")
        val mongo = mongoFactory(mongoConfig)
        val database = mongo.database(mongoConfig.getString("database").getOrElse("analytics"))

        val tokensCollection  = mongoConfig.getString("tokensCollection").getOrElse("tokens")

        val tokenManager      = new TokenManager(database, tokensCollection)

        AnalyticsState(aggregationEngine, tokenManager)
      } ->
        request { state =>
        import state._

        }
      }
  }
}

