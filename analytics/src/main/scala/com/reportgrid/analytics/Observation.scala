package com.reportgrid.analytics

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.Printer._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._

import org.joda.time.Instant
import scala.annotation.tailrec
import scalaz.Semigroup
import scalaz.Scalaz._

case class Variable(name: JPath)
object Variable {
  implicit val orderingVariable: Ordering[Variable] = new Ordering[Variable] {
    override def compare(v1: Variable, v2: Variable) = {
      v1.name.toString.compare(v2.name.toString)
    }
  }
}

sealed abstract class Observation
case class HasValue(variable: Variable, value: JValue) extends Observation
case class HasChild(variable: Variable, child: JPathNode) extends Observation

case class JointObservation[A <: Observation](obs: Set[A]) {
  def order = obs.size

  def of[B >: A <: Observation] = JointObservation[B](obs.toSet[B])
}

object JointObservation {
  def apply[A <: Observation](a: A*): JointObservation[A] = JointObservation(a.toSet)
}

object JointObservations {
  /** Creates a report of values.
   */
  def ofValues(event: JValue, order: Int, depth: Int, limit: Int): (Set[JointObservation[HasValue]], Set[HasValue]) = {
    val (infinite, finite) = event.flattenWithPath.take(limit).map {
      case (jpath, jvalue) => HasValue(Variable(jpath), jvalue)
    } partition {
      case HasValue(Variable(jpath), _) => jpath.endsInInfiniteValueSpace
    }
  
    (combinationsTo(finite, order), infinite.toSet)
  }

  /** Creates a report of children. Although the "order" parameter is supported,
   * it's recommended to always use a order = 1, because higher order counts do
   * not contain much additional information.
   */
  def ofChildren(event: JValue, order: Int): Set[JointObservation[HasChild]] = {
    val flattened = event.foldDownWithPath(List.empty[HasChild]) { (l, jpath, _) =>
      jpath.parent.map(p => HasChild(Variable(p), jpath.nodes.last) :: l).getOrElse(l)
    }

    combinationsTo(flattened, order)
  }

  def ofInnerNodes(event: JValue, order: Int): Set[JointObservation[HasChild]] = {
    val flattened = event.foldDownWithPath(List.empty[HasChild]) { (l, jpath, jvalue) =>
      jvalue match {
        case JNothing | JNull | JBool(_) | JInt(_) | JDouble(_) | JString(_) => l
          // exclude the path when the jvalue indicates a leaf node
        case _ =>
          jpath.parent.map(p => HasChild(Variable(p), jpath.nodes.last) :: l).getOrElse(l)
      }
    }

    combinationsTo(flattened, order)
  }

  private def combinationsTo[A <: Observation](l: Seq[A], order: Int): Set[JointObservation[A]] = {
    (for (i <- (1 to order); obs <- l.combinations(i)) yield JointObservation(obs.toSet))(collection.breakOut)
  }
}

case class Tag(name: String, value: TagValue) 

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
            case AnonLocation(p) => JString(p.path)
            case NamedLocation(name, p) => JObject(JField(name, JString(p.path)) :: Nil) 
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
              Hierarchy.of(lefts.map(t => NamedLocation(t._1, Path(t._2)))).getOrElse {
                sys.error("Tag value levels do not respect the refinement rule.")
              }
            } else {
              Hierarchy.of(rights.map(v => AnonLocation(Path(v)))).getOrElse(NameSet(rights.toSet))
            }
          )

        case _ => sys.error("Illegal tag format: " + renderNormalized(v))
      }
    }
  }
}

sealed trait TagValue {
  type StorageKeysType <: StorageKeys
  def storageKeys: List[StorageKeysType]
}

case class NameSet(values: Set[String]) extends TagValue {
  type StorageKeysType = NameSetKeys
  override def storageKeys = for (name <- values.toList) yield NameSetKeys(name, name)
}

case class TimeReference(encoding: TimeSeriesEncoding, time: Instant) extends TagValue {
  type StorageKeysType = TimeRefKeys
  override def storageKeys = for ((k, v) <- encoding.grouped(time)) yield TimeRefKeys(k, v)
}

case class Hierarchy private (locations: List[Hierarchy.Location]) extends TagValue {
  type StorageKeysType = HierarchyKeys
  override def storageKeys = for (l <- locations; parent <- l.path.parent) yield HierarchyKeys(parent, l.path)
}

object Hierarchy {
  sealed trait Location {
    def path: Path
  }

  case class AnonLocation(path: Path) extends Location
  case class NamedLocation(name: String, path: Path) extends Location

  def of[T <: Location](locations: List[T]) = {
    (respectsRefinementRule(locations.map(_.path))).option(Hierarchy(locations.sortBy(_.path.length)))
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

sealed trait StorageKeys {
  type DocKey
  type DataKey
  def docKey: DocKey
  def dataKey: DataKey
}

case class NameSetKeys(docKey: String, dataKey: String) extends StorageKeys {
  type DocKey = String
  type DataKey = String
}

case class TimeRefKeys(docKey: (Periodicity, Period), dataKey: Instant) extends StorageKeys {
  type DocKey = (Periodicity, Period)
  type DataKey = Instant
}

case class HierarchyKeys(docKey: Path, dataKey: Path) extends StorageKeys {
  type DocKey = Path
  type DataKey = Path
}
// vim: set ts=4 sw=4 et:
