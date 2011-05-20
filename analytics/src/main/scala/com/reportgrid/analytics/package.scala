package com.reportgrid

import blueeyes.json.JsonAST._
import blueeyes.json.JPath
import blueeyes.json.Printer._

import org.joda.time.{DateTime, DateTimeZone}

package object analytics extends AggregatorImplicits {
  type Observation[A <: Predicate] = Set[(Variable, A)]

  type ObservationCounted[A <: Predicate, B] = (Observation[A], B)

  def cleanPath(string: String): String = "/" + string.split("/").map(_.trim).filter(_.length > 0).mkString("/")

  /** Finds sublists of the specified list up to the specified order.
   */
  def sublists[T](l: List[T], order: Int): List[List[T]] = {
    if (order <= 0) Nil 
    else l match {
      case Nil => Nil

      case x :: xs =>
        val smallerSets = sublists(xs, order)

        List(List(x)) ++ smallerSets ++ (smallerSets.map(x :: _).filter(_.length <= order))
    }
  }

  /** Returns all unique combinations resulting from choosing one element from
   * each list.
   */
  def chooseAll[T](list: List[List[T]]): List[List[T]] = list match {
    case Nil => Nil

    case x :: Nil => x.map(List(_))

    case x :: xs =>
      val remainder = chooseAll(xs)

      x.flatMap { element =>
        remainder.map(element :: _)
      }
  }

  /** Normalizes the JValue, basically by sorting according to a default JValue ordering.
   */
  def normalize[T <: JValue](jvalue: T): T = {
    import blueeyes.json.xschema.DefaultOrderings.JValueOrdering

    jvalue match {
      case JObject(fields) => JObject(fields.map(normalize _).sorted(JValueOrdering)).asInstanceOf[T]

      case JArray(elements) => JArray(elements.map(normalize _).sorted(JValueOrdering)).asInstanceOf[T]

      case JField(name, value) => JField(name, normalize(value)).asInstanceOf[T]

      case _ => jvalue
    }
  }

  /** Renders a normalized JValue.
   */
  def renderNormalized(jvalue: JValue): String = compact(render(normalize(jvalue)))

  def flatten(list: List[Map[JPath, JValue]]): Map[JPath, List[JValue]] = {
    list.foldLeft[Map[JPath, List[JValue]]](Map()) { (all, cur) =>
      cur.foldLeft(all) { (all, entry: (JPath, JValue)) =>
        val (jpath, jvalue) = entry

        all.updated(jpath, all.get(jpath).getOrElse(Nil) :+ jvalue)
      }
    }
  }
}
