package com.reportgrid.analytics

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import java.io._
import org.joda.time.DateTime
import Periodicity._

trait SignatureGen[-T] extends (T => Array[Byte])
object SignatureGen extends SignatureGens

trait SignatureGens {
  private def toBytes(size: Int, f: DataOutputStream => Unit): Array[Byte] = {
    val byteArray = new ByteArrayOutputStream(size)
    val dataOutputStream = new DataOutputStream(byteArray)
    f(dataOutputStream)
    byteArray.toByteArray
  }

  def genSignature[T](t: T)(implicit f: SignatureGen[T]): Array[Byte] = f(t)

  implicit object StringSignatureGen extends SignatureGen[String] {
    val TypeSig = "String".getBytes
    override def apply(v: String) = TypeSig ++ v.getBytes
  }

  implicit def genSignatureTuple2[A: SignatureGen, B: SignatureGen] = new Tuple2SignatureGen[A, B]
  class Tuple2SignatureGen[A: SignatureGen, B: SignatureGen] extends SignatureGen[(A, B)] {
    val TypeSig = genSignature("Tuple2") 
    override def apply(v: (A, B)): Array[Byte] = TypeSig ++ genSignature(v._1) ++ genSignature(v._2)
  }

  implicit object PeriodicitySignatureGen extends SignatureGen[Periodicity] {
    val TypeSig = genSignature("Periodicity") 
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
    val TypeSig = genSignature("Long") 
    override def apply(v: Long) = TypeSig ++ toBytes(8, (_: DataOutputStream).writeLong(v))
  }

  implicit object IntSignatureGen extends SignatureGen[Int] {
    val TypeSig = genSignature("Int") 
    override def apply(v: Int) = TypeSig ++ toBytes(4, (_: DataOutputStream).writeInt(v))
  }

  implicit object DoubleSignatureGen extends SignatureGen[Double] {
    val TypeSig = genSignature("Double") 
    override def apply(v: Double) = TypeSig ++ toBytes(8, (_: DataOutputStream).writeDouble(v))
  }

  implicit object BigIntSignatureGen extends SignatureGen[BigInt] {
    val TypeSig = genSignature("BigInt") 
    override def apply(v: BigInt) = TypeSig ++ v.toString.getBytes
  }

  implicit object BooleanSignatureGen extends SignatureGen[Boolean] {
    val TypeSig = genSignature("Boolean") 
    override def apply(v: Boolean) = TypeSig ++ toBytes(4, (_: DataOutputStream).writeBoolean(v))
  }

  implicit object PathSignatureGen extends SignatureGen[Path] {
    val TypeSig = genSignature("Path") 
    override def apply(v: Path) = TypeSig ++ genSignature(v.path)
  }

  implicit object DateTimeSignatureGen extends SignatureGen[DateTime] {
    val TypeSig = genSignature("DateTime") 
    override def apply(v: DateTime) = TypeSig ++ genSignature(v.getMillis)
  }

  implicit object PeriodSignatureGen extends SignatureGen[Period] {
    val TypeSig = genSignature("Period") 
    override def apply(p: Period) = TypeSig ++ genSignature(p.periodicity) ++ genSignature(p.start)
  }

  implicit object TokenSignatureGen extends SignatureGen[Token] {
    val TypeSig = genSignature("Token") 
    override def apply(v: Token) = TypeSig ++ genSignature(v.accountTokenId)
  }

  implicit def genSignatureSet[T](implicit evidence: SignatureGen[T]) = new SetSignatureGen[T]
  class SetSignatureGen[T](implicit ts: SignatureGen[T]) extends SignatureGen[Set[T]] {
    val TypeSig = genSignature("Set") 
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

  implicit def genSignatureSeq[T: SignatureGen, M[T] <: Seq[T]] = new SeqSignatureGen[T, M]
  class SeqSignatureGen[T: SignatureGen, M[T] <: Seq[T]] extends SignatureGen[M[T]] {
    val TypeSig = genSignature("Seq")
    override def apply(v: M[T]) = TypeSig ++ (v.view.map(implicitly[SignatureGen[T]]).flatten)
  }

  implicit object JPathSignatureGen extends SignatureGen[JPath] {
    val TypeSig = genSignature("JPath")
    override def apply(v: JPath) = TypeSig ++ genSignature(v.toString)
  }

  implicit object JPathNodeSignatureGen extends SignatureGen[JPathNode] {
    val TypeSig = genSignature("JPathNode")
    override def apply(v: JPathNode) = TypeSig ++ genSignature(v.toString)
  }

  implicit object JValueSignatureGen extends SignatureGen[JValue] {
    val JObjectTypeSig  = genSignature("JObject")
    val JArrayTypeSig   = genSignature("JArray")
    val JStringTypeSig  = genSignature("JString")
    val JBoolTypeSig    = genSignature("JBool")
    val JIntTypeSig     = genSignature("JInt")
    val JDoubleTypeSig  = genSignature("JDouble")
    val JNullTypeSig    = genSignature("JNull")
    val JNothingTypeSig = genSignature("JNothing")

    override def apply(v: JValue) = v match {
      case JObject(fields)  => JObjectTypeSig  ++ genSignatureSet(JFieldSignatureGen)(fields.toSet)
      case JArray(elements) => JArrayTypeSig   ++ genSignature(elements)
      case JString(v)       => JStringTypeSig  ++ genSignature(v)
      case JBool(v)         => JBoolTypeSig    ++ genSignature(v)
      case JInt(v)          => JIntTypeSig     ++ genSignature(v)
      case JDouble(v)       => JDoubleTypeSig  ++ genSignature(v)
      case JNull            => JNullTypeSig    ++ Array(0: Byte)
      case JNothing         => JNothingTypeSig ++ Array(Byte.MaxValue)
      case _ => error("JField shouldn't be a JValue")
    }
  }

  implicit object JFieldSignatureGen extends SignatureGen[JField] {
    val TypeSig = genSignature("JField") 
    override def apply(v: JField) = TypeSig ++ genSignature((v.name, v.value))
  }

  implicit object VariableSignatureGen extends SignatureGen[Variable] {
    val TypeSig = genSignature("Variable") 
    override def apply(v: Variable) = TypeSig ++ genSignature(v.name)
  }

  implicit object HasChildSignatureGen extends SignatureGen[HasChild] {
    val TypeSig = genSignature("HasChild") 
    override def apply(v: HasChild) = TypeSig ++ genSignature(v.child)
  }

  implicit object HasValueSignatureGen extends SignatureGen[HasValue] {
    val TypeSig = genSignature("HasValue") 
    override def apply(v: HasValue) = TypeSig ++ genSignature(v.value)
  }
}



// vim: set ts=4 sw=4 et:
