package com.reportgrid.billing

import java.security.Security
import com.braintreegateway.{BraintreeGateway, Environment}
import net.lag.configgy._
import blueeyes.BlueEyesServer
import blueeyes.persistence.mongo.{Mongo, MockMongo, RealMongo}
import blueeyes.core.service.ServerHealthMonitorService
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.core.service.HttpClient
import blueeyes.core.data.ByteChunk

import com.reportgrid.billing._
import com.reportgrid.billing.braintree.BraintreeService

object BillingServer extends BlueEyesServer with NewBillingService with ServerHealthMonitorService {

  override def httpClient: HttpClient[ByteChunk] = new HttpClientXLightWeb
  
  override def mailerFactory(config: ConfigMap) = new NullMailer
  
  override def accountsFactory(config: ConfigMap) = {
    val mongoConfig = config.configMap("mongo")
    val mongo = mongoFactory(mongoConfig)

    val databaseName = mongoConfig.getString("database")
    if(databaseName.isEmpty) throw new IllegalArgumentException("Accounts database setting required")
    
    val accountsCollection = mongoConfig.getString("collection")
    if(accountsCollection.isEmpty) throw new IllegalArgumentException("Accounts collection setting required")
    
    val database = mongo.database(databaseName.get)
    val billingService = braintreeFactory(config.configMap("braintree"))
    val tokenGenerator = tokenGeneratorFactory(config.configMap("tokenGenerator"))
        
    new Accounts(config.configMap("accounts"), tokenGenerator, billingService, database, accountsCollection.get)
  }

  def mongoFactory(config: ConfigMap): Mongo = {
    new RealMongo(config)
  }
  
  def braintreeFactory(config: ConfigMap): BraintreeService = {
    val env = getConfigSetting(config, "environment") match {
      case x if x.toUpperCase() == "SANDBOX"    => Environment.SANDBOX
      case x if x.toUpperCase() == "PRODUCTION" => Environment.PRODUCTION
      case x                                    => throw new IllegalArgumentException("Invalid braintree environment setting: " + x)
    }
    
    val merchantId = getConfigSetting(config, "merchantId")
    val publicKey = getConfigSetting(config, "publicKey")
    val privateKey = getConfigSetting(config, "privateKey")
    
    val gateway = new BraintreeGateway(
      env,
      merchantId,
      publicKey,
      privateKey);
    
    new BraintreeService(gateway, env)
  }
  
  private def getConfigSetting(config: ConfigMap, key: String): String = {
    config.getString(key) match {
      case Some(x) => x
      case None    => throw new IllegalArgumentException("Braintree %s setting required.".format(key))
    }
  }
  
  def tokenGeneratorFactory(config: ConfigMap) = {
    new MockTokenGenerator
  }
  
  override def main(args: Array[String]) = {
    super.main(args)
  }
}