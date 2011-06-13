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

  implicit def genSignatureTuple2[A: SignatureGen, B: SignatureGen] = new Tuple2SignatureGen[A, B]
  class Tuple2SignatureGen[A: SignatureGen, B: SignatureGen] extends SignatureGen[(A, B)] {
    override def apply(v: (A, B)): Array[Byte] = genSignature(v._1) ++ genSignature(v._2)
  }

  implicit object StringSignatureGen extends SignatureGen[String] {
    override def apply(v: String) = v.getBytes
  }

  implicit object PeriodicitySignatureGen extends SignatureGen[Periodicity] {
    override def apply(v: Periodicity) = Array(v match {
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
    override def apply(v: Long) = toBytes(8, (_: DataOutputStream).writeLong(v))
  }

  implicit object IntSignatureGen extends SignatureGen[Int] {
    override def apply(v: Int) = toBytes(4, (_: DataOutputStream).writeInt(v))
  }

  implicit object DoubleSignatureGen extends SignatureGen[Double] {
    override def apply(v: Double) = toBytes(8, (_: DataOutputStream).writeDouble(v))
  }

  implicit object BigIntSignatureGen extends SignatureGen[BigInt] {
    override def apply(v: BigInt) = v.toString.getBytes
  }

  implicit object BooleanSignatureGen extends SignatureGen[Boolean] {
    override def apply(v: Boolean) = toBytes(4, (_: DataOutputStream).writeBoolean(v))
  }

  implicit object PathSignatureGen extends SignatureGen[Path] {
    override def apply(v: Path) = genSignature(v.path)
  }

  implicit object DateTimeSignatureGen extends SignatureGen[DateTime] {
    override def apply(v: DateTime) = genSignature(v.getMillis)
  }

  implicit object PeriodSignatureGen extends SignatureGen[Period] {
    override def apply(p: Period) = genSignature(p.periodicity) ++ genSignature(p.start)
  }

  implicit object TokenSignatureGen extends SignatureGen[Token] {
    override def apply(v: Token) = genSignature(v.accountTokenId)
  }

  implicit def genSignatureSet[T](implicit evidence: SignatureGen[T]) = new SetSignatureGen[T]
  class SetSignatureGen[T](implicit ts: SignatureGen[T]) extends SignatureGen[Set[T]] {
    override def apply(v: Set[T]) = if (v.isEmpty) {
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
    }
  }

  implicit def genSignatureSeq[T: SignatureGen, M[T] <: Seq[T]] = new SeqSignatureGen[T, M]
  class SeqSignatureGen[T: SignatureGen, M[T] <: Seq[T]] extends SignatureGen[M[T]] {
    override def apply(v: M[T]) = v.view.map(implicitly[SignatureGen[T]]).flatten.toArray
  }

  implicit object JPathSignatureGen extends SignatureGen[JPath] {
    override def apply(v: JPath) = genSignature(v.toString)
  }

  implicit object JPathNodeSignatureGen extends SignatureGen[JPathNode] {
    override def apply(v: JPathNode) = genSignature(v.toString)
  }

  implicit object JValueSignatureGen extends SignatureGen[JValue] {
    override def apply(v: JValue) = v match {
      case JObject(fields) => genSignatureSet(JFieldSignatureGen)(fields.toSet)
      case JArray(elements) => genSignature(elements)
      case JString(v) => genSignature(v)
      case JBool(v) => genSignature(v)
      case JInt(v) => genSignature(v)
      case JDouble(v) => genSignature(v)
      case JNull => Array(0: Byte)
      case JNothing => Array(Byte.MaxValue)
      case _ => error("JField shouldn't be a JValue")
    }
  }

  implicit object JFieldSignatureGen extends SignatureGen[JField] {
    override def apply(v: JField) = genSignature((v.name, v.value))
  }

  implicit object VariableSignatureGen extends SignatureGen[Variable] {
    override def apply(v: Variable) = genSignature(v.name)
  }

  implicit object HasChildSignatureGen extends SignatureGen[HasChild] {
    override def apply(v: HasChild) = genSignature(v.child)
  }

  implicit object HasValueSignatureGen extends SignatureGen[HasValue] {
    override def apply(v: HasValue) = genSignature(v.value)
  }
}



// vim: set ts=4 sw=4 et:
