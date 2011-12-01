package com.reportgrid.analytics

import Periodicity._
import com.reportgrid.common.HashFunction

import blueeyes._
import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._

import java.nio.ByteBuffer
import org.joda.time.Instant
import org.apache.commons.codec.binary.Hex

import scalaz.Scalaz._

class Sig(val length: Int, _message: => String)(val write: ByteBuffer => Unit) {
  lazy val message = _message
  lazy val bytes = (ByteBuffer.allocate(length) ->- write).array

  def :+ (w: Sig): Sig = Sig(length + w.length, message + "; " + w.message) {
    (b: ByteBuffer) => write(b); w.write(b)
  }

  def :+ (a: Array[Byte]) = Sig(length + a.length, a.toString) { _.put(a) }
  def :+ (i: Int) =         Sig(length + 4, i.toString) { _.putInt(i) }
  def :+ (l: Long) =        Sig(length + 8, l.toString) { _.putLong(l) }
  def :+ (d: Double) =      Sig(length + 8, d.toString) { _.putDouble(d) }
  def :+ (b: Byte) =        Sig(length + 1, b.toString) { _.put(b) }
  def :+ (b: Boolean): Sig = this :+ (if (b) 1 : Byte else 0 : Byte)

  def hashSignature(implicit hashFunction: HashFunction) = {
    Hex.encodeHexString(hashFunction(bytes))
  }
}

object Sig {
  def apply(s: String): Sig = s.getBytes("UTF-8") |> (bytes => Sig(bytes.length, s) { _.put(bytes) })
  def apply(a: Array[Byte]): Sig = Sig(a.length, a.toString) { _.put(a) }
  def apply(length: Int, _message: => String)(write: ByteBuffer => Unit) = new Sig(length, _message)(write)
  def apply(sigs: Sig*): Sig = sigs.reduceLeft(_ :+ _)
}

class Signed[T](v: T)(implicit gen: SignatureGen[T]) {
  def sig: Sig = gen(v)
}

trait SignatureGen[-T] extends (T => Sig)
object SignatureGen {
  implicit def signed[T: SignatureGen](v: T): Signed[T] = new Signed(v)

  @inline final def sig[T](t: T)(implicit f: SignatureGen[T]): Sig = f(t)

  implicit object IdentitySignatureGen extends SignatureGen[Sig] {
    override def apply(v: Sig) = v
  }

  implicit object StringSignatureGen extends SignatureGen[String] {
    final val TypeSig = Sig("String")
    override def apply(v: String) = TypeSig :+ v.getBytes("UTF-8")
  }

  implicit def genTuple2Signature[A: SignatureGen, B: SignatureGen] = new Tuple2SignatureGen[A, B]
  class Tuple2SignatureGen[A: SignatureGen, B: SignatureGen] extends SignatureGen[(A, B)] {
    final val TypeSig = Sig("Tuple2")
    override def apply(v: (A, B)) = Sig(TypeSig, sig(v._1), sig(v._2))
  }

  implicit object PeriodicitySignatureGen extends SignatureGen[Periodicity] {
    final val TypeSig = Sig("Periodicity")
    override def apply(v: Periodicity) = TypeSig :+ v.byteValue
  }

  implicit object ArrayByteSignatureGen extends SignatureGen[Array[Byte]] {
    override def apply(v: Array[Byte]) = Sig(v)
  }

  implicit object LongSignatureGen extends SignatureGen[Long] {
    final val TypeSig = Sig("Long".getBytes("UTF-8") )
    override def apply(v: Long) = TypeSig :+ v
  }

  implicit object IntSignatureGen extends SignatureGen[Int] {
    final val TypeSig = Sig("Int")
    override def apply(v: Int) = TypeSig :+ v
  }

  implicit object DoubleSignatureGen extends SignatureGen[Double] {
    final val TypeSig = Sig("Double")
    override def apply(v: Double) = TypeSig :+ v
  }

  implicit object BigIntSignatureGen extends SignatureGen[BigInt] {
    final val TypeSig = Sig("BigInt")
    override def apply(v: BigInt) = TypeSig :+ v.toByteArray
  }

  implicit object BooleanSignatureGen extends SignatureGen[Boolean] {
    final val TypeSig = Sig("Boolean")
    override def apply(v: Boolean) = TypeSig :+ v
  }

  implicit object PathSignatureGen extends SignatureGen[Path] {
    final val TypeSig = Sig("Path")
    override def apply(v: Path) = TypeSig :+ v.path.getBytes("UTF-8")
  }

  implicit object InstantSignatureGen extends SignatureGen[Instant] {
    final val TypeSig = Sig("Instant") 
    override def apply(v: Instant) = TypeSig :+ v.getMillis
  }

  implicit object PeriodSignatureGen extends SignatureGen[Period] {
    final val TypeSig = Sig("Period") 
    override def apply(p: Period) = TypeSig :+ p.periodicity.byteValue :+ p.start.getMillis
  }

  implicit object TokenSignatureGen extends SignatureGen[Token] {
    final val TypeSig = Sig("Token") 
    override def apply(v: Token) = TypeSig :+ v.accountTokenId.getBytes("UTF-8")
  }

  implicit def genSetSignature[T: SignatureGen] = new SetSignatureGen[T]
  class SetSignatureGen[T](implicit ts: SignatureGen[T]) extends SignatureGen[Set[T]] {
    final val TypeSig = Sig("Set") 
    override def apply(v: Set[T]) = if (v.isEmpty) TypeSig else {
      import com.reportgrid.util.Bytes
      import scala.collection.JavaConverters._
      TypeSig :+ Bytes.add(v.toIterable.map(ts(_).bytes).asJava)
    }
  }

  implicit def genSeqSignature[T: SignatureGen, M[T] <: Seq[T]] = new SeqSignatureGen[T, M]
  class SeqSignatureGen[T, M[T] <: Seq[T]](implicit gen: SignatureGen[T]) extends SignatureGen[M[T]] {
    final val TypeSig = Sig("Seq")
    override def apply(v: M[T]) = Sig(TypeSig +: v.map(gen): _*)
  }

  implicit object JPathSignatureGen extends SignatureGen[JPath] {
    final val TypeSig = Sig("JPath")
    override def apply(v: JPath) = TypeSig :+ v.toString.getBytes("UTF-8")
  }

  implicit object JPathNodeSignatureGen extends SignatureGen[JPathNode] {
    final val TypeSig = Sig("JPathNode")
    override def apply(v: JPathNode) = TypeSig :+ v.toString.getBytes("UTF-8")
  }

  implicit object JValueSignatureGen extends SignatureGen[JValue] {
    final val JObjectTypeSig  = Sig("JObject")
    final val JArrayTypeSig   = Sig("JArray")
    final val JStringTypeSig  = Sig("JString")
    final val JBoolTypeSig    = Sig("JBool")
    final val JIntTypeSig     = Sig("JInt")
    final val JDoubleTypeSig  = Sig("JDouble")
    final val JNullTypeSig    = Sig("JNull")
    final val JNothingTypeSig = Sig("JNothing")

    override def apply(v: JValue) = v match {
      case JObject(fields)  => JObjectTypeSig :+ genSetSignature(JFieldSignatureGen)(fields.toSet)
      case JArray(elements) => JArrayTypeSig  :+ sig(elements)
      case JString(v)       => JStringTypeSig :+ v.getBytes("UTF-8")
      case JBool(v)         => JBoolTypeSig   :+ v 
      case JInt(v)          => JIntTypeSig    :+ v
      case JDouble(v)       => JDoubleTypeSig :+ v
      case JNull            => JNullTypeSig
      case JNothing         => JNothingTypeSig
      case _ => sys.error("JField shouldn't be a JValue")
    }
  }

  implicit object JFieldSignatureGen extends SignatureGen[JField] {
    final val TypeSig = Sig("JField") 
    override def apply(v: JField) = Sig(TypeSig, Sig(v.name.getBytes("UTF-8")), sig(v.value))
  }

  implicit object VariableSignatureGen extends SignatureGen[Variable] {
    final val TypeSig = Sig("Variable") 
    override def apply(v: Variable) = TypeSig :+ sig(v.name)
  }

  implicit object HasChildSignatureGen extends SignatureGen[HasChild] {
    final val TypeSig = Sig("HasChild")
    override def apply(v: HasChild) = Sig(TypeSig, sig(v.variable), sig(v.child))
  }

  implicit object HasValueSignatureGen extends SignatureGen[HasValue] {
    final val TypeSig = Sig("HasValue")
    override def apply(v: HasValue) = Sig(TypeSig, sig(v.variable), sig(v.value))
  }

  implicit def JointObservationSignatureGen[A <: Observation : SignatureGen]: SignatureGen[JointObservation[A]] = new SignatureGen[JointObservation[A]] {
    final val TypeSig = Sig("JointObservation")
    override def apply(v: JointObservation[A]) = sig(v.obs)
  }
}



// vim: set ts=4 sw=4 et:
