package com.reportgrid.analytics

import blueeyes.concurrent.Future
import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.Printer._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.JodaSerializationImplicits._

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
    val (infinite, finite) = event.flattenWithPath.filter(_._1.length <= depth).take(limit).map {
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
  val Prefix = "#"
  private val pattern = "^" + Prefix + "*"
  def tname(s: String) = s.replaceAll(pattern, Prefix)

  sealed trait ExtractionResult
  case object Skipped extends ExtractionResult
  case class Tags(tags: Future[Seq[Tag]]) extends ExtractionResult
  case class Errors(errors: Seq[ExtractionError]) extends ExtractionResult

  case class ExtractionError(fieldName: String, message: String) {
    override def toString = "An error occurred parsing tag " + fieldName + ": " + message
  }

  type TagExtractor = JObject => (ExtractionResult, JObject)

  class RichTagExtractor(ex: TagExtractor) {
    def or (other: TagExtractor): TagExtractor = (o: JObject) => ex(o) match {
      case result @ (Tags(tags), remainder) => other(remainder) match {
        case (Tags(rest), remainder) => (Tags(tags |+| rest), remainder)
        case (Skipped, _) => result
        case error => error
      }

      case err @ (Errors(errors), remainder) => other(remainder) match {
        case (Tags(tags), remainder) => (Tags(tags), remainder)
        case (Errors(rest), remainder) => (Errors(errors |+| rest), remainder)
        case _ => err
      }

      case (Skipped, remainder) => other(remainder)
    }
  } 

  implicit def richTagExtractor(ex: TagExtractor) = new RichTagExtractor(ex)

  val TimestampProperty = tname("timestamp")

  def timeTagExtractor(encoding: TimeSeriesEncoding, auto: => Instant, alwaysTrack: Boolean): TagExtractor = (o: JObject) => {
    val remainder = JObject(o.fields.filter(_.name != TimestampProperty))
    def skippedResult = (Skipped, remainder)
    def autoResult = (Tags(Future.sync(Tag("timestamp", TimeReference(encoding, auto)) :: Nil)), remainder)

    (o \ TimestampProperty) match {
      case JBool(false)     => skippedResult
      case JBool(true)      => autoResult
      case JNothing | JNull => if (alwaysTrack) autoResult else skippedResult

      case jvalue => extractTimestampTag(encoding, "timestamp", jvalue) match {
        case tags: Tags => (tags, remainder)
        case x => (x, o)
      }    
    }
  }

  def extractTimestampTag(encoding: TimeSeriesEncoding, tagName: String, jvalue: JValue) = jvalue.validated[Instant].fold(
    error => Errors(ExtractionError(tagName, error.message) :: Nil), 
    instant => Tags(Future.sync(Tag(tagName, TimeReference(encoding, instant)) :: Nil))
  )

  val LocationProperty = tname("location")

  def locationTagExtractor(auto: => Future[Option[Hierarchy]]) = (o: JObject) => {
    val remainder = JObject(o.fields.filter(_.name != LocationProperty))
    (o \ LocationProperty) match {
      case JNothing | JNull | JBool(false) => (Skipped, o)
      case JBool(true) | JString("auto") => (Tags(auto.map(_.map(Tag("location", _)).toSeq)), remainder)
      case x => extractHierarchyTag(LocationProperty, x) match {
        case tags: Tags => (tags, remainder)
        case other => (other, o)
      }
    }
  }

  def extractHierarchyTag(tagName: String, v: JValue): ExtractionResult = {
    def result(locations: List[Hierarchy.Location]) = Hierarchy.of(locations).fold(
      error => Errors(ExtractionError(tagName, error) :: Nil),
      hierarchy => Tags(Future.sync(Tag(tagName, hierarchy) :: Nil))
    )

    v match {
      case JArray(elements) => 
        val locations = elements.collect {
          case JString(pathName) => Hierarchy.AnonLocation(Path(pathName))
        }

        if (locations.size == elements.size) result(locations) else Skipped

      case JObject(fields) => 
        val locations = fields.collect {
          case JField(name, JString(pathName)) => Hierarchy.NamedLocation(name, Path(pathName))
        }
        
        if (locations.size == fields.size) result(locations) else Skipped
      
      case _ => Skipped
    }
  }

  def extractTags(extractors: List[TagExtractor], event: JObject): (ExtractionResult, JObject) = {
    extractors.reduceLeft(_ or _).apply(event) match {
      case (result, remainder) => 
        val unparsed = remainder.fields.filter(_.name.startsWith(Tag.Prefix)) 
        if (unparsed.isEmpty) (result, remainder)
        else (Errors(unparsed.map(field => ExtractionError(field.name, "No extractor could handle the value: " + compact(render(field.value))))), remainder)
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

  def grouped(time: Instant): List[((Periodicity, Period), Instant)] = {
    (for ((k, v) <- encoding.grouping) yield (k, v.period(time)) -> k.floor(time))(collection.breakOut)
  }

  override def storageKeys = for ((k, v) <- grouped(time)) yield TimeRefKeys(k, v)
}

case class Hierarchy private (locations: List[Hierarchy.Location]) extends TagValue {
  type StorageKeysType = HierarchyKeys
  override def storageKeys = for (l <- locations; parent <- l.path.parent) yield HierarchyKeys(parent, l.path)
}

object Hierarchy {
  sealed trait Location {
    def path: Path
  }

  object Location {
    implicit object LocationExtractor extends Extractor[Location] {
      def extract(v: JValue): Location = v match {
        case JString(path) => AnonLocation(Path(path))
        case JObject(List(JField(name, JString(path)))) => NamedLocation(name, Path(path))
        case x => sys.error("Cannot deserialize a Location from " + pretty(render(x)))
      }
    }
  }

  case class AnonLocation(path: Path) extends Location
  case class NamedLocation(name: String, path: Path) extends Location

  def of[T <: Location](locations: List[T]) = {
    (respectsRefinementRule(locations.map(_.path)))
    .option(Hierarchy(locations.sortBy(_.path.length)))
    .toSuccess("The specified list of locations " + locations + " does not respect the refinement rule.")
  }

  def respectsRefinementRule(values: List[Path]): Boolean = {
    @tailrec def parallel(l: List[List[String]], acc: Boolean): Boolean = {
      val (heads, tails) = l.foldLeft((List.empty[String], List.empty[List[String]])) {
        case ((heads, tails), x :: xs) => (x :: heads, xs :: tails)
        case (ht, Nil) => ht
      }

      if (heads.isEmpty) acc 
      else parallel(tails, acc && heads.distinct.size == 1)
    }

    values.map(_.length).distinct.size == values.size && parallel(values.map(_.elements), true)
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
