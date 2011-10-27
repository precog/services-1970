package com.reportgrid.billing

import java.security.SecureRandom

import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.codec.binary.Hex

object PasswordHash {

  val saltBytes = 20

  def saltedHash(password: String): String = {
    val salt = generateSalt
    val hash = hashWithSalt(salt, password)
    salt + hash
  }

  def generateSalt(): String = {
    val random = new SecureRandom()
    val salt = random.generateSeed(saltBytes)
    Hex.encodeHexString(salt).toUpperCase()
  }

  def checkSaltedHash(password: String, saltedHash: String): Boolean = {
    val saltLength = saltBytes * 2
    val salt = saltedHash.substring(0, saltLength)
    val hash = saltedHash.substring(saltLength)
    val testHash = hashWithSalt(salt, password)
    hash == testHash
  }

  private def hashWithSalt(salt: String, password: String): String = {
    hashFunction(salt + password)
  }

  private def hashFunction(password: String): String = {
    DigestUtils.shaHex(password).toUpperCase()
  }
}
