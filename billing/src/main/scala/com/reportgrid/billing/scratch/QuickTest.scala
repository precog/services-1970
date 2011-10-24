package com.reportgrid.billing.scratch

import scalaz._
import scalaz.Scalaz._

import net.lag.configgy._

import blueeyes.persistence.mongo._
import blueeyes.json.Printer
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser

import com.reportgrid.billing.SerializationHelpers._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.SerializationImplicits._
import blueeyes.json.xschema._
import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.Extractor._
import com.reportgrid.billing.BillingServiceHandlers._
import com.reportgrid.billing._

import java.security.SecureRandom
import org.apache.commons.codec.binary.Hex

object QuickTest {

  def main(args: Array[String]): Unit = {

    val emailPattern = "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,4}".r

    def validEmailAddress(e: String): Boolean = {
      emailPattern.pattern.matcher(e).matches()
    }
    
    println(validEmailAddress("a@b.com"))
    println(validEmailAddress("abc.com"))

  }

  private def parseIt(string: String) = {
    val json = JsonParser.parse(string)
    val foo: CreateAccount = json.deserialize[CreateAccount]
  }

  private def previous: Unit = {
    val c = CreateAccount(
      Signup("a@b.com",
        Some("b co"),
        Some("b.com"),
        "starter",
        None,
        "hash"),
      None)

    val string = Printer.pretty(Printer.render(c.serialize))
    println(string)
    parseIt(string)

    val s2 = """
        {"email":"b@b.com","company":"b","website":"b.com","planId":"starter","password":"",
        "billing":{"cardholder":"","number":"","expMonth":"","expYear":"","cvv":"","postalCode":""},
        "passwordHash":"e99a18c428cb38d5f260853678922e03"}
      """

    val s3 = """
        {"email":"","company":"","website":"b.com","planId":"starter","password":"",
        "passwordHash":"e99a18c428cb38d5f260853678922e03"}
      """

    val s4 = """
        {"email":"b@b.com","company":"b","website":"b.com","planId":"starter","password":"",
        "billing":{"cardholder":"","number":"","expMonth":"1","expYear":"1","cvv":"","postalCode":""},
        "passwordHash":"e99a18c428cb38d5f260853678922e03"}
      """

    parseIt(s4)
  }

  private def extractedMethod: Unit = {
    1.to(1000).foreach(j => {
      1.to(100).foreach(i => {
        val r = new SecureRandom();
        val p1 = Hex.encodeHexString(r.generateSeed(20))
        val p2 = Hex.encodeHexString(r.generateSeed(20))

        val h1 = PasswordHash.saltedHash(p1)
        if (!PasswordHash.checkSaltedHash(p1, h1) || PasswordHash.checkSaltedHash(p2, h1)) {
          print("F")
        } else {
          print(".")
        }
      })
      println
    })
    println("Done")
  }

  private def extractedMethod2: Unit = {

    val ca1 = CreateAccount(
      Signup(
        "a@b.com",
        Some("b co"),
        Some("b.co"),
        "starter",
        None,
        "abc123"),
      None)

    val ca2 = CreateAccount(
      Signup(
        "g@h.com",
        None,
        None,
        "bronze",
        None,
        "abc123"),
      None)

    val ca3 = CreateAccount(
      Signup(
        "g@h.com",
        None,
        None,
        "bronze",
        None,
        "abc123"),
      Some(BillingInformation(
        "George Harmon",
        "4111111111111111",
        5,
        2012,
        "123",
        "60607")))

  }
}