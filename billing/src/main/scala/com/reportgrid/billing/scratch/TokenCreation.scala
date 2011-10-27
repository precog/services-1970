package com.reportgrid.billing.scratch

import blueeyes.core.service.HttpClient
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.core.data.ByteChunk
import blueeyes.core.data.BijectionsChunkString._
import blueeyes.core.data.BijectionsChunkJson._
import blueeyes.concurrent.Future
import blueeyes.core.http.HttpResponse
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser

import com.reportgrid.billing.RealTokenGenerator
import blueeyes.core.http.MimeTypes._
import com.reportgrid.analytics.Token

import scalaz._
import scalaz.Scalaz._

object TokenCreation {

  val httpClient: HttpClient[ByteChunk] = new HttpClientXLightWeb
  val url = "http://api.reportgrid.com/services/analytics/v1/tokens/"

  def main(args: Array[String]) = {
    val token = Token.Test.tokenId
    val tg = new RealTokenGenerator(httpClient, token, url)

    //dumpTokens(token)

    val nt = tg.newToken("/billing/test")

    println(nt)
    
    //dumpTokens(token)

//      val df = tg.deleteToken("617B8CEA-DBCF-4225-B8A0-5D7A84A278AD")    
    val df: Future[Unit] = nt.flatMap {
      case Success(t) => {
        //tg.deleteToken(t)
        Future.sync(Unit)
      }
      case _ => Future.sync(Unit)
    }

    while (!df.isDone) {}

    //dumpTokens(token)
  }
  
  def newToken(parent: String): String = {
    val foo: Future[HttpResponse[JValue]] = httpClient.query("tokenId", parent).contentType[ByteChunk](application / json).post[JValue](url)(JsonParser.parse("""
          {
            "path": , "%s"
            "permissions": {
                "read":    true,
                "write":   true,
                "share":   true,
                "explore": true
            },
            "limits": {
                "order": %d,
                "limit": %d,
                "depth": %d,
                "tags" : %d
            }
          }
        """.format("com_reportgrid_nick", 2, 10, 3, 1)))
    
    while(!foo.isDone) {
      
    }
    
    val jv = foo.value.get.content.get
    jv.asInstanceOf[JString].value
  }
  
  def removeToken(parent: String, token: String): Unit = {
    val foo: Future[HttpResponse[JValue]] = httpClient.query("tokenId", parent).contentType[ByteChunk](application / json).delete[JValue](url + token)
    while(!foo.isDone) {
      
    }
    Unit
  }

  def numberOfChildren(t: String): Int = {
    children(t).size
  }

  def children(t: String): List[JValue] = {
    try {
      val foo: Future[HttpResponse[JValue]] = httpClient.query("tokenId", t).contentType[ByteChunk](application / json).get[JValue](url)

      while (!foo.isDone) {

      }

      val content = foo.value.get.content.get

      val jarr = content.asInstanceOf[JArray]

      jarr.children
    } catch {
      case ex => Nil
    }
  }
  
  private def dumpTokens(token: String): Unit = {
      
    println(numberOfChildren(token))
    
    children(token).foreach{ c =>
      val cval = c.asInstanceOf[JString].value
      println(cval)
      //println("  " + cval + ": " + numberOfChildren(cval))
    }
  }
}