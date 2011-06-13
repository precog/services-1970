package com.reportgrid.analytics

import org.apache.commons.codec.binary.Hex

trait HashFunction extends (Array[Byte] => Array[Byte])

object Sha1HashFunction extends HashFunction {
  override def apply(bytes : Array[Byte]) : Array[Byte] = {
    val hash = java.security.MessageDigest.getInstance("SHA-1")
    hash.reset()
    hash.update(bytes)
    hash.digest()
  }
}

object Hashable {
  def hashSignature[T](t: T)(implicit genSignature: SignatureGen[T], hashFunction: HashFunction): String = {
    Hex.encodeHexString(hashFunction(genSignature(t)))
  }
}


// vim: set ts=4 sw=4 et:
