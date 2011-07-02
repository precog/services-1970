package com.reportgrid.analytics

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import java.io._
import org.joda.time.Instant
import Periodicity._

trait SignatureGen[-T] extends (T => Array[Byte])
object SignatureGen extends SignatureGens {
  implicit def signed[T: SignatureGen](v: T): Signed[T] = new Signed(v)
}

class Signed[T: SignatureGen](v: T) {
  def sig: Array[Byte] = implicitly[SignatureGen[T]].apply(v)
}


trait SignatureGens {
  private def toBytes(size: Int, f: DataOutputStream => Unit): Array[Byte] = {
    val byteArray = new ByteArrayOutputStream(size)
    val dataOutputStream = new DataOutputStream(byteArray)
    f(dataOutputStream)
    byteArray.toByteArray
  }

  def sig[T](t: T)(implicit f: SignatureGen[T]): Array[Byte] = f(t)

  implicit object StringSignatureGen extends SignatureGen[String] {
    val TypeSig = "String".getBytes
    override def apply(v: String) = TypeSig ++ v.getBytes
  }

  implicit def genTuple2Signature[A: SignatureGen, B: SignatureGen] = new Tuple2SignatureGen[A, B]
  class Tuple2SignatureGen[A: SignatureGen, B: SignatureGen] extends SignatureGen[(A, B)] {
    val TypeSig = sig("Tuple2") 
    override def apply(v: (A, B)): Array[Byte] = TypeSig ++ sig(v._1) ++ sig(v._2)
  }

  implicit object PeriodicitySignatureGen extends SignatureGen[Periodicity] {
    val TypeSig = sig("Periodicity") 
    override def apply(v: Periodicity) = TypeSig ++ Array(v match {
      case Second   => 0: Byte
      case Minute   => 1: Byte
      case Hour     => 2: Byte
      case Day      => 3: Byte
      case Week     => 4: Byte
      case Month    => 5: Byte
      case Year     => 6: Byte
      case Eternity => Byte.MaxValue
    })
  }

  implicit object ArrayByteSignatureGen extends SignatureGen[Array[Byte]] {
    override def apply(v: Array[Byte]): Array[Byte] = v
  }

  implicit object LongSignatureGen extends SignatureGen[Long] {
    val TypeSig = sig("Long") 
    override def apply(v: Long) = TypeSig ++ toBytes(8, (_: DataOutputStream).writeLong(v))
  }

  implicit object IntSignatureGen extends SignatureGen[Int] {
    val TypeSig = sig("Int") 
    override def apply(v: Int) = TypeSig ++ toBytes(4, (_: DataOutputStream).writeInt(v))
  }

  implicit object DoubleSignatureGen extends SignatureGen[Double] {
    val TypeSig = sig("Double") 
    override def apply(v: Double) = TypeSig ++ toBytes(8, (_: DataOutputStream).writeDouble(v))
  }

  implicit object BigIntSignatureGen extends SignatureGen[BigInt] {
    val TypeSig = sig("BigInt") 
    override def apply(v: BigInt) = TypeSig ++ v.toString.getBytes
  }

  implicit object BooleanSignatureGen extends SignatureGen[Boolean] {
    val TypeSig = sig("Boolean") 
    override def apply(v: Boolean) = TypeSig ++ toBytes(4, (_: DataOutputStream).writeBoolean(v))
  }

  implicit object PathSignatureGen extends SignatureGen[Path] {
    val TypeSig = sig("Path") 
    override def apply(v: Path) = TypeSig ++ sig(v.path)
  }

  implicit object InstantSignatureGen extends SignatureGen[Instant] {
    val TypeSig = sig("Instant") 
    override def apply(v: Instant) = TypeSig ++ sig(v.getMillis)
  }

  implicit object PeriodSignatureGen extends SignatureGen[Period] {
    val TypeSig = sig("Period") 
    override def apply(p: Period) = TypeSig ++ sig(p.periodicity) ++ sig(p.start)
  }

  implicit object TokenSignatureGen extends SignatureGen[Token] {
    val TypeSig = sig("Token") 
    override def apply(v: Token) = TypeSig ++ sig(v.accountTokenId)
  }

  implicit def genSetSignature[T: SignatureGen] = new SetSignatureGen[T]
  class SetSignatureGen[T](implicit ts: SignatureGen[T]) extends SignatureGen[Set[T]] {
    val TypeSig = sig("Set") 
    override def apply(v: Set[T]) = TypeSig ++ (if (v.isEmpty) {
      Array[Byte]()
    } else {
      val bytes = v.map(ts).toSeq
      val target = new Array[Byte](bytes.map(_.length).max)
      var i, j = 0
      while (j < bytes.size) {
        i = 0
        var a = bytes(j)
        while (i < a.size) {
          target(i) = target(i).+(a(i)).toByte
          i += 1
        }
        j += 1
      }
      
      target
    })
  }

  implicit def genSeqSignature[T: SignatureGen, M[T] <: Seq[T]] = new SeqSignatureGen[T, M]
  class SeqSignatureGen[T: SignatureGen, M[T] <: Seq[T]] extends SignatureGen[M[T]] {
    val TypeSig = sig("Seq")
    override def apply(v: M[T]) = TypeSig ++ (v.view.map(implicitly[SignatureGen[T]]).flatten)
  }

  implicit object JPathSignatureGen extends SignatureGen[JPath] {
    val TypeSig = sig("JPath")
    override def apply(v: JPath) = TypeSig ++ sig(v.toString)
  }

  implicit object JPathNodeSignatureGen extends SignatureGen[JPathNode] {
    val TypeSig = sig("JPathNode")
    override def apply(v: JPathNode) = TypeSig ++ sig(v.toString)
  }

  implicit object JValueSignatureGen extends SignatureGen[JValue] {
    val JObjectTypeSig  = sig("JObject")
    val JArrayTypeSig   = sig("JArray")
    val JStringTypeSig  = sig("JString")
    val JBoolTypeSig    = sig("JBool")
    val JIntTypeSig     = sig("JInt")
    val JDoubleTypeSig  = sig("JDouble")
    val JNullTypeSig    = sig("JNull")
    val JNothingTypeSig = sig("JNothing")

    override def apply(v: JValue) = v match {
      case JObject(fields)  => JObjectTypeSig  ++ genSetSignature(JFieldSignatureGen)(fields.toSet)
      case JArray(elements) => JArrayTypeSig   ++ sig(elements)
      case JString(v)       => JStringTypeSig  ++ sig(v)
      case JBool(v)         => JBoolTypeSig    ++ sig(v)
      case JInt(v)          => JIntTypeSig     ++ sig(v)
      case JDouble(v)       => JDoubleTypeSig  ++ sig(v)
      case JNull            => JNullTypeSig    ++ Array(0: Byte)
      case JNothing         => JNothingTypeSig ++ Array(Byte.MaxValue)
      case _ => sys.error("JField shouldn't be a JValue")
    }
  }

  implicit object JFieldSignatureGen extends SignatureGen[JField] {
    val TypeSig = sig("JField") 
    override def apply(v: JField) = TypeSig ++ sig((v.name, v.value))
  }

  implicit object VariableSignatureGen extends SignatureGen[Variable] {
    val TypeSig = sig("Variable") 
    override def apply(v: Variable) = TypeSig ++ sig(v.name)
  }

  implicit object HasChildSignatureGen extends SignatureGen[HasChild] {
    val TypeSig = sig("HasChild") 
    override def apply(v: HasChild) = TypeSig ++ sig(v.child)
  }

  implicit object HasValueSignatureGen extends SignatureGen[HasValue] {
    val TypeSig = sig("HasValue") 
    override def apply(v: HasValue) = TypeSig ++ sig(v.value)
  }
}



// vim: set ts=4 sw=4 et:
