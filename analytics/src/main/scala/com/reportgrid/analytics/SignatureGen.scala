package com.reportgrid.analytics

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import java.nio.ByteBuffer
import org.joda.time.Instant
import Periodicity._

class Signed[T](v: T)(implicit gen: SignatureGen[T]) {
  def sig: Array[Byte] = gen(v)
}

trait SignatureGen[-T] extends (T => Array[Byte])
object SignatureGen {
  implicit def signed[T: SignatureGen](v: T): Signed[T] = new Signed(v)

  @inline final def sig[T](t: T)(implicit f: SignatureGen[T]): Array[Byte] = f(t)

  @inline final def typed(typeSig: Array[Byte], bytes: Array[Byte]): Array[Byte] = {
    ByteBuffer.allocate(typeSig.length + bytes.length).put(typeSig).put(bytes).array
  }

  @inline final def typed(typeSig: Array[Byte], long: Long): Array[Byte] = {
    ByteBuffer.allocate(typeSig.length + 8).put(typeSig).putLong(long).array
  }

  @inline final def typed(typeSig: Array[Byte], int: Int): Array[Byte] = {
    ByteBuffer.allocate(typeSig.length + 4).put(typeSig).putInt(int).array
  }

  @inline final def typed(typeSig: Array[Byte], double: Double): Array[Byte] = {
    ByteBuffer.allocate(typeSig.length + 8).put(typeSig).putDouble(double).array
  }

  @inline final def typed(typeSig: Array[Byte], bool: Boolean): Array[Byte] = {
    ByteBuffer.allocate(typeSig.length + 1).put(typeSig).put(if (bool) 1 : Byte else 0 : Byte).array
  }

  implicit object StringSignatureGen extends SignatureGen[String] {
    final val TypeSig = "String".getBytes
    override def apply(v: String) = typed(TypeSig, v.getBytes)
  }

  implicit def genTuple2Signature[A: SignatureGen, B: SignatureGen] = new Tuple2SignatureGen[A, B]
  class Tuple2SignatureGen[A: SignatureGen, B: SignatureGen] extends SignatureGen[(A, B)] {
    final val TypeSig = "Tuple2".getBytes 
    override def apply(v: (A, B)): Array[Byte] = {
      val v1s = sig(v._1)
      val v2s = sig(v._2)
      ByteBuffer.allocate(TypeSig.length + v1s.length + v2s.length).put(TypeSig).put(v1s).put(v2s).array
    }
  }

  implicit object PeriodicitySignatureGen extends SignatureGen[Periodicity] {
    final val TypeSig = "Periodicity".getBytes 
    override def apply(v: Periodicity) = ByteBuffer.allocate(TypeSig.length + 1).put(TypeSig).put(v.byteValue).array
  }

  implicit object ArrayByteSignatureGen extends SignatureGen[Array[Byte]] {
    override def apply(v: Array[Byte]): Array[Byte] = v
  }

  implicit object LongSignatureGen extends SignatureGen[Long] {
    final val TypeSig = "Long".getBytes 
    override def apply(v: Long) = typed(TypeSig, v)
  }

  implicit object IntSignatureGen extends SignatureGen[Int] {
    final val TypeSig = "Int".getBytes 
    override def apply(v: Int) = typed(TypeSig, v)
  }

  implicit object DoubleSignatureGen extends SignatureGen[Double] {
    final val TypeSig = "Double".getBytes 
    override def apply(v: Double) = typed(TypeSig, v)
  }

  implicit object BigIntSignatureGen extends SignatureGen[BigInt] {
    final val TypeSig = "BigInt".getBytes 
    override def apply(v: BigInt) = typed(TypeSig, v.toByteArray)
  }

  implicit object BooleanSignatureGen extends SignatureGen[Boolean] {
    final val TypeSig = "Boolean".getBytes 
    override def apply(v: Boolean) = typed(TypeSig, v)
  }

  implicit object PathSignatureGen extends SignatureGen[Path] {
    final val TypeSig = "Path".getBytes 
    override def apply(v: Path) = typed(TypeSig, v.path.getBytes)
  }

  implicit object InstantSignatureGen extends SignatureGen[Instant] {
    final val TypeSig = "Instant".getBytes 
    override def apply(v: Instant) = typed(TypeSig, v.getMillis)
  }

  implicit object PeriodSignatureGen extends SignatureGen[Period] {
    final val TypeSig = "Period".getBytes 
    override def apply(p: Period) = {
      ByteBuffer.allocate(TypeSig.length + 1 + 8).put(TypeSig).put(p.periodicity.byteValue).putLong(p.start.getMillis).array
    }
  }

  implicit object TokenSignatureGen extends SignatureGen[Token] {
    final val TypeSig = "Token".getBytes 
    override def apply(v: Token) = typed(TypeSig, v.accountTokenId.getBytes)
  }

  implicit def genSetSignature[T: SignatureGen] = new SetSignatureGen[T]
  class SetSignatureGen[T](implicit ts: SignatureGen[T]) extends SignatureGen[Set[T]] {
    final val TypeSig = "Set".getBytes 
    override def apply(v: Set[T]) = if (v.isEmpty) TypeSig else {
      import com.reportgrid.util.Bytes
      import scala.collection.JavaConverters._

      typed(TypeSig, Bytes.add(v.toIterable.map(ts).asJava))
    }
  }

  implicit def genSeqSignature[T: SignatureGen, M[T] <: Seq[T]] = new SeqSignatureGen[T, M]
  class SeqSignatureGen[T, M[T] <: Seq[T]](implicit gen: SignatureGen[T]) extends SignatureGen[M[T]] {
    final val TypeSig = "Seq".getBytes
    override def apply(v: M[T]) = {
      val sigs = v.map(gen)
      val buf = ByteBuffer.allocate(TypeSig.length + sigs.map(_.length).sum).put(TypeSig);
      sigs.foreach(buf.put)
      buf.array
    }
  }

  implicit object JPathSignatureGen extends SignatureGen[JPath] {
    final val TypeSig = "JPath".getBytes
    override def apply(v: JPath) = typed(TypeSig, v.toString.getBytes)
  }

  implicit object JPathNodeSignatureGen extends SignatureGen[JPathNode] {
    final val TypeSig = "JPathNode".getBytes
    override def apply(v: JPathNode) = typed(TypeSig, v.toString.getBytes)
  }

  implicit object JValueSignatureGen extends SignatureGen[JValue] {
    final val JObjectTypeSig  = "JObject".getBytes
    final val JArrayTypeSig   = "JArray".getBytes
    final val JStringTypeSig  = "JString".getBytes
    final val JBoolTypeSig    = "JBool".getBytes
    final val JIntTypeSig     = "JInt".getBytes
    final val JDoubleTypeSig  = "JDouble".getBytes
    final val JNullTypeSig    = "JNull".getBytes
    final val JNothingTypeSig = "JNothing".getBytes

    override def apply(v: JValue) = v match {
      case JObject(fields)  => typed(JObjectTypeSig, genSetSignature(JFieldSignatureGen)(fields.toSet))
      case JArray(elements) => typed(JArrayTypeSig, sig(elements))
      case JString(v)       => typed(JStringTypeSig, v.getBytes)
      case JBool(v)         => typed(JBoolTypeSig, v) 
      case JInt(v)          => typed(JIntTypeSig, v)
      case JDouble(v)       => typed(JDoubleTypeSig, v)
      case JNull            => JNullTypeSig
      case JNothing         => JNothingTypeSig
      case _ => sys.error("JField shouldn't be a JValue")
    }
  }

  implicit object JFieldSignatureGen extends SignatureGen[JField] {
    final val TypeSig = "JField".getBytes 
    override def apply(v: JField) = {
      val nsig = v.name.getBytes
      val vsig = sig(v.value)
      ByteBuffer.allocate(TypeSig.length + nsig.length + vsig.length).put(TypeSig).put(nsig).put(vsig).array
    }
  }

  implicit object VariableSignatureGen extends SignatureGen[Variable] {
    final val TypeSig = "Variable".getBytes 
    override def apply(v: Variable) = typed(TypeSig, sig(v.name))
  }

  implicit object PredicateSignatureGen extends SignatureGen[Predicate] {
    final val CSig = "HasChild".getBytes
    final val VSig = "HadValue".getBytes
    override def apply(v: Predicate) = v match {
      case HasChild(child) => typed(CSig, sig(child))
      case HasValue(value) => typed(VSig, sig(value))
    }
  }
}



// vim: set ts=4 sw=4 et:
