package com.reportgrid.analytics

import Periodicity._

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._

import java.nio.ByteBuffer
import org.joda.time.Instant
import org.apache.commons.codec.binary.Hex


case class Sig(bytes: Array[Byte]) {
  def :+ (s: Sig) = Sig(ByteBuffer.allocate(bytes.length + s.bytes.length).put(bytes).put(s.bytes).array)
  def :+ (a: Array[Byte]) = Sig(ByteBuffer.allocate(bytes.length + a.length).put(bytes).put(a).array)
  def :+ (l: Int) = Sig(ByteBuffer.allocate(bytes.length + 4).put(bytes).putInt(l).array)
  def :+ (l: Long) = Sig(ByteBuffer.allocate(bytes.length + 8).put(bytes).putLong(l).array)
  def :+ (d: Double) = Sig(ByteBuffer.allocate(bytes.length + 8).put(bytes).putDouble(d).array)
  def :+ (b: Byte) = Sig(ByteBuffer.allocate(bytes.length + 1).put(bytes).put(b).array)
  def :+ (b: Boolean): Sig = this :+ (if (b) 1 : Byte else 0 : Byte)

  def hashSignature(implicit hashFunction: HashFunction) = {
    Hex.encodeHexString(hashFunction(bytes))
  }
}

object Sig {
  def apply(sigs: Sig*): Sig = {
    val buf = ByteBuffer.allocate(sigs.map(_.bytes.length).sum)
    for (sig <- sigs) buf.put(sig.bytes)
    Sig(buf.array)
  }
}

class Signed[T](v: T)(implicit gen: SignatureGen[T]) {
  def sig: Sig = gen(v)
}

trait SignatureGen[-T] extends (T => Sig)
object SignatureGen {
  implicit def signed[T: SignatureGen](v: T): Signed[T] = new Signed(v)

  @inline final def sig[T](t: T)(implicit f: SignatureGen[T]): Sig = Sig(f(t))

  implicit object IdentitySignatureGen extends SignatureGen[Sig] {
    override def apply(v: Sig) = v
  }

  implicit object StringSignatureGen extends SignatureGen[String] {
    final val TypeSig = Sig("String".getBytes)
    override def apply(v: String) = TypeSig :+ v.getBytes
  }

  implicit def genTuple2Signature[A: SignatureGen, B: SignatureGen] = new Tuple2SignatureGen[A, B]
  class Tuple2SignatureGen[A: SignatureGen, B: SignatureGen] extends SignatureGen[(A, B)] {
    final val TypeSig = Sig("Tuple2".getBytes)
    override def apply(v: (A, B)) = Sig(TypeSig, sig(v._1), sig(v._2))
  }

  implicit object PeriodicitySignatureGen extends SignatureGen[Periodicity] {
    final val TypeSig = Sig("Periodicity".getBytes)
    override def apply(v: Periodicity) = TypeSig :+ v.byteValue
  }

  implicit object ArrayByteSignatureGen extends SignatureGen[Array[Byte]] {
    override def apply(v: Array[Byte]) = Sig(v)
  }

  implicit object LongSignatureGen extends SignatureGen[Long] {
    final val TypeSig = Sig("Long".getBytes )
    override def apply(v: Long) = TypeSig :+ v
  }

  implicit object IntSignatureGen extends SignatureGen[Int] {
    final val TypeSig = Sig("Int".getBytes)
    override def apply(v: Int) = TypeSig :+ v
  }

  implicit object DoubleSignatureGen extends SignatureGen[Double] {
    final val TypeSig = Sig("Double".getBytes)
    override def apply(v: Double) = TypeSig :+ v
  }

  implicit object BigIntSignatureGen extends SignatureGen[BigInt] {
    final val TypeSig = Sig("BigInt".getBytes)
    override def apply(v: BigInt) = TypeSig :+ v.toByteArray
  }

  implicit object BooleanSignatureGen extends SignatureGen[Boolean] {
    final val TypeSig = Sig("Boolean".getBytes)
    override def apply(v: Boolean) = TypeSig :+ v
  }

  implicit object PathSignatureGen extends SignatureGen[Path] {
    final val TypeSig = Sig("Path".getBytes)
    override def apply(v: Path) = TypeSig :+ v.path.getBytes
  }

  implicit object InstantSignatureGen extends SignatureGen[Instant] {
    final val TypeSig = Sig("Instant".getBytes) 
    override def apply(v: Instant) = TypeSig :+ v.getMillis
  }

  implicit object PeriodSignatureGen extends SignatureGen[Period] {
    final val TypeSig = Sig("Period".getBytes) 
    override def apply(p: Period) = TypeSig :+ p.periodicity.byteValue :+ p.start.getMillis
  }

  implicit object TokenSignatureGen extends SignatureGen[Token] {
    final val TypeSig = Sig("Token".getBytes) 
    override def apply(v: Token) = TypeSig :+ v.accountTokenId.getBytes
  }

  implicit def genSetSignature[T: SignatureGen] = new SetSignatureGen[T]
  class SetSignatureGen[T](implicit ts: SignatureGen[T]) extends SignatureGen[Set[T]] {
    final val TypeSig = Sig("Set".getBytes) 
    override def apply(v: Set[T]) = if (v.isEmpty) TypeSig else {
      import com.reportgrid.util.Bytes
      import scala.collection.JavaConverters._
      TypeSig :+ Bytes.add(v.toIterable.map(ts(_).bytes).asJava)
    }
  }

  implicit def genSeqSignature[T: SignatureGen, M[T] <: Seq[T]] = new SeqSignatureGen[T, M]
  class SeqSignatureGen[T, M[T] <: Seq[T]](implicit gen: SignatureGen[T]) extends SignatureGen[M[T]] {
    final val TypeSig = Sig("Seq".getBytes)
    override def apply(v: M[T]) = Sig(v.map(gen): _*)
  }

  implicit object JPathSignatureGen extends SignatureGen[JPath] {
    final val TypeSig = Sig("JPath".getBytes)
    override def apply(v: JPath) = TypeSig :+ v.toString.getBytes
  }

  implicit object JPathNodeSignatureGen extends SignatureGen[JPathNode] {
    final val TypeSig = Sig("JPathNode".getBytes)
    override def apply(v: JPathNode) = TypeSig :+ v.toString.getBytes
  }

  implicit object JValueSignatureGen extends SignatureGen[JValue] {
    final val JObjectTypeSig  = Sig("JObject".getBytes)
    final val JArrayTypeSig   = Sig("JArray".getBytes)
    final val JStringTypeSig  = Sig("JString".getBytes)
    final val JBoolTypeSig    = Sig("JBool".getBytes)
    final val JIntTypeSig     = Sig("JInt".getBytes)
    final val JDoubleTypeSig  = Sig("JDouble".getBytes)
    final val JNullTypeSig    = Sig("JNull".getBytes)
    final val JNothingTypeSig = Sig("JNothing".getBytes)

    override def apply(v: JValue) = v match {
      case JObject(fields)  => JObjectTypeSig :+ genSetSignature(JFieldSignatureGen)(fields.toSet)
      case JArray(elements) => JArrayTypeSig  :+ sig(elements)
      case JString(v)       => JStringTypeSig :+ v.getBytes
      case JBool(v)         => JBoolTypeSig   :+ v 
      case JInt(v)          => JIntTypeSig    :+ v
      case JDouble(v)       => JDoubleTypeSig :+ v
      case JNull            => JNullTypeSig
      case JNothing         => JNothingTypeSig
      case _ => sys.error("JField shouldn't be a JValue")
    }
  }

  implicit object JFieldSignatureGen extends SignatureGen[JField] {
    final val TypeSig = Sig("JField".getBytes) 
    override def apply(v: JField) = Sig(TypeSig, Sig(v.name.getBytes), sig(v.value))
  }

  implicit object VariableSignatureGen extends SignatureGen[Variable] {
    final val TypeSig = Sig("Variable".getBytes) 
    override def apply(v: Variable) = TypeSig :+ sig(v.name)
  }

  implicit object HasChildSignatureGen extends SignatureGen[HasChild] {
    final val TypeSig = Sig("HasChild".getBytes)
    override def apply(v: HasChild) = Sig(TypeSig, sig(v.variable), sig(v.child))
  }

  implicit object HasValueSignatureGen extends SignatureGen[HasValue] {
    final val TypeSig = Sig("HasValue".getBytes)
    override def apply(v: HasValue) = Sig(TypeSig, sig(v.variable), sig(v.value))
  }

  implicit def JointObservationSignatureGen[A <: Observation : SignatureGen]: SignatureGen[JointObservation[A]] = new SignatureGen[JointObservation[A]] {
    final val TypeSig = Sig("JointObservation".getBytes)
    override def apply(v: JointObservation[A]) = sig(v.obs)
  }
}



// vim: set ts=4 sw=4 et:
