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

object BillingServer extends BlueEyesServer with BillingService with ServerHealthMonitorService {

  override def httpClient: HttpClient[ByteChunk] = new HttpClientXLightWeb
  
  override def notificationsFactory(config: ConfigMap) = {
    val url = "https://sendgrid.com/api/mail.send.json?"
    val apiUser = "operations@reportgrid.com"
    val apiKey = "seGrid8"

    val mailer = new SendGridMailer(httpClient, url, apiUser, apiKey)
    new MailerNotificationSender(mailer)
  }
  
  override def usageClientFactory(config: ConfigMap) = {
    val baseUrl = getConfigSetting("usageService", "baseUrl", config)
    new RealUsageClient(httpClient, baseUrl)
  }
  
  override def accountsFactory(config: ConfigMap) = {
    val mongoConfig = config.configMap("mongo")
    val mongo = mongoFactory(mongoConfig)

    val databaseName = getConfigSetting("Mongo", "database", mongoConfig)
    val accountsCollection = getConfigSetting("Mongo", "collection", mongoConfig)
    
    val database = mongo.database(databaseName)
    val usageClient = usageClientFactory(config.configMap("usageService"))
    val billingService = braintreeFactory(config.configMap("braintree"))
    val tokenGenerator = tokenGeneratorFactory(config.configMap("tokenGenerator"))
        
    new PrivateAccounts(
        config.configMap("accounts"), 
        new MongoAccountInformationStore(database, accountsCollection), 
        billingService, 
        usageClient, 
        notificationsFactory(config.configMap("notifications")), 
        tokenGenerator)
  }

  def mongoFactory(config: ConfigMap): Mongo = {
    new RealMongo(config)
  }
  
  def braintreeFactory(config: ConfigMap): BraintreeService = {
    val env = getConfigSetting("Braintree", "environment", config) match {
      case x if x.toUpperCase() == "SANDBOX"    => Environment.SANDBOX
      case x if x.toUpperCase() == "PRODUCTION" => Environment.PRODUCTION
      case x                                    => sys.error("Braintree: invalid environment setting: " + x)
    }
    
    val merchantId = getConfigSetting("Braintree", "merchantId", config)
    val publicKey = getConfigSetting("Braintree", "publicKey", config)
    val privateKey = getConfigSetting("Braintree", "privateKey", config)
    
    val gateway = new BraintreeGateway(
      env,
      merchantId,
      publicKey,
      privateKey);
    
    new BraintreeService(gateway, env)
  }
  
  private def getConfigSetting(prefix: String, key: String, config: ConfigMap): String = {
    config.getString(key).getOrElse(sys.error("%s: %s setting required.".format(prefix, key)))
  }
  
  def tokenGeneratorFactory(config: ConfigMap) = {
    val rootToken = getConfigSetting("TokenGenerator", "rootToken", config)
    val rootUrl = getConfigSetting("TokenGenerator", "rootUrl", config)

    new RealTokenGenerator(httpClient, rootToken, rootUrl)
  }
  
  override def main(args: Array[String]) = {
    super.main(args)
  }
}