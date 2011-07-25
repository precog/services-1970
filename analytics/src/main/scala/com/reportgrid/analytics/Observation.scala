package com.reportgrid.analytics

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.Printer._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._

import scala.annotation.tailrec
import scalaz.Scalaz._

case class Variable(name: JPath)
object Variable {
  implicit val orderingVariable: Ordering[Variable] = new Ordering[Variable] {
    override def compare(v1: Variable, v2: Variable) = {
      v1.name.toString.compare(v2.name.toString)
    }
  }
}

sealed trait Observation {
  def order: Int
  def + (obs: Observartion*) = JointObservation.of(obs :+ this: _*)
  def tags: Set[Tag] 
  def predicates: Set[Predicate]
}


case class JointObservation private (obs: Set[Observation]) extends Observation {
  lazy val order = obs.toSeq.map(_.order).sum
  override def tags = obs.flatMap(_.tags)
  override def predicates = obs.flatMap(_.predicates)
}

object JointObservation {
  def of(obs: Observation*): JointObservation = {
    def flat(obs: Observation, acc: Set[Observation]): Set[Observation] = obs match {
      case JointObservation(complex) => complex.flatMap(flat(_, acc))
      case simple => acc + simple
    }

    JointObservation(obs.flatMap(flat(_, Set())))
  }
}

sealed trait Predicate extends Observation
case class HasValue(variable: Variable, value: JValue) extends Observation with Predicate {
  override def tags = Set.empty[Tag]
  override def predicates = Set(this)
  val order = 1
}

case class HasChild(variable: Variable, value: JPathNode) extends Observation with Predicate {
  override def tags = Set.empty[Tag]
  override def predicates = Set(this)
  val order = 1
}

case class Tag(name: String, value: TagValue) extends Observation {
  override def tags = Set(this)
  override def predicates = Set.empty[Predicate]
  val order = 1
}

object Tag {
  import Hierarchy._

  implicit object TagDecomposer extends Decomposer[Tag] {
    def decompose(v: Tag): JValue = v match {
      case Tag(name, NameSet(values)) => JObject(
        JField("#" + name, values.serialize) :: Nil
      )

      case Tag(name, Hierarchy(values)) => JObject(
        JField("#" + name, JArray(
          values map {
            case AnonLocation(value) => JString(value.mkString("/"))
            case NamedLocation(name, value) => JObject(JField(name, JString(value.mkString("/"))) :: Nil) 
          }
        )) :: Nil
      )

      case _ => error("todo")
    }
  }

  implicit object TagExtractor extends Extractor[Tag] {
    def extract(v: JValue): Tag = {
      v match {
        case JObject(JField(name, JArray(tags)) :: Nil) if name.startsWith("#") =>
          val tagName = name.drop(1)
          val (lefts, rights) = tags.foldLeft((List.empty[(String, String)], List.empty[String])) {
            case ((lefts, rights), JObject(JField(lodName, JString(value)) :: Nil)) => ((lodName, value) :: lefts, rights)
            case ((lefts, rights), JString(value)) => (lefts, value :: rights)
            case _ => sys.error("Bad tag format: tags must be either strings or objects with a single field each.")
          } 

          if (lefts.isEmpty ^ rights.isEmpty) { //xor to the rescue
            sys.error("You cannot match labeled and unlabeled level-of-detail tags.")
          } 
          
          Tag(
            tagName,
            if (rights.isEmpty) {
              Hierarchy.of(lefts.map(t => NamedLocation(t._1, t._2.split("/").toList))).getOrElse {
                sys.error("Tag value levels do not respect the refinement rule.")
              }
            } else {
              Hierarchy.of(rights.map(v => AnonLocation(v.split("/").toList))).getOrElse(NameSet(rights.toSet))
            }
          )

        case _ => sys.error("Illegal tag format: " + renderNormalized(v))
      }
    }
  }
}

sealed trait TagValue {
  type DocKey
  type DataKey
  case class StorageKey(docKey: DocKey, dataKey: DataKey)

  def storageKeys: List[StorageKey]
}

case class NameSet(values: Set[String]) extends TagValue {
  type DocKey = String
  type DataKey = String
  def storageKeys = for (name <- values) yield StorageKey(name, name)
}

case class TimeReference(encoding: TimeSeriesEncoding, time: Instant) extends TagValue {
  type DocKey = (Periodicity, Period)
  type DataKey = Instant
  def storageKeys = for ((k, v) <- encoding.grouped(time)) yield StorageKey(k, v)
}

case class Hierarchy private (locations: List[Hierarchy.Location]) extends TagValue {
  type DocKey = Path
  type DataKey = Path
  def storageKeys = for (p <- locations) yield StorageKey(p.parent, p)
}

object Hierarchy {
  sealed class Location(
    def value: Path
  }

  case class AnonLocation(value: Path) extends Location
  case class NamedLocation(name: String, value: Path) extends Location

  def of[T <: Location](locations: List[T]) = {
    (respectsRefinementRule(locations.map(_.value))).option(Hierarchy(locations.sortBy(_.value.length)))
  }

  def respectsRefinementRule(values: List[Path]): Boolean = {
    @tailrec def parallel(l: List[List[String]], acc: Boolean): Boolean = {
      val (heads, tails) = l.foldLeft((List.empty[String], List.empty[List[String]])) {
        case ((heads, tails), x :: xs) => (x :: heads, xs :: tails)
        case (ht, Nil) => ht
      }

      if (tails.isEmpty) acc && heads.distinct.size == 1
      else parallel(tails, acc && heads.distinct.size == 1)
    }

    values.map(_.length).distinct.size == 1 && parallel(values.map(_.elements), true)
  }
}

// vim: set ts=4 sw=4 et:
