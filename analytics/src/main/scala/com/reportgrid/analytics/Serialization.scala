package com.reportgrid.analytics


import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.JodaSerializationImplicits._

import org.joda.time.DateTime
import scalaz.Scalaz._
import scalaz.Validation

/**
 * Extractors and decomposers for types where the same serialization format is shared
 * beween the service API and MongoDB
 */
trait AnalyticsSerialization {
  final implicit val JPathDecomposer: Decomposer[JPath] = new Decomposer[JPath] {
    def decompose(v: JPath): JValue = v.toString.serialize
  }

  final implicit val JPathExtractor: Extractor[JPath] = new Extractor[JPath] {
    def extract(v: JValue): JPath = JPath(v.deserialize[String])
  }

  final implicit val JPathNodeDecomposer: Decomposer[JPathNode] = new Decomposer[JPathNode] {
    def decompose(v: JPathNode): JValue = v.toString.serialize
  }

  final implicit val JPathNodeExtractor: Extractor[JPathNode] = new Extractor[JPathNode] {
    def extract(v: JValue): JPathNode = {
      JPath(v.deserialize[String]).nodes match {
        case node :: Nil => node
        case _ => sys.error("Too many or few nodes to extract JPath node from " + v)
      }
    }
  }

  final implicit val PeriodicityDecomposer = new Decomposer[Periodicity] {
    def decompose(periodicity: Periodicity): JValue = periodicity.name
  }

  final implicit val PeriodicityExtractor = new Extractor[Periodicity] with ValidatedExtraction[Periodicity] {
    override def validated(value: JValue): Validation[Extractor.Error, Periodicity] = {
      val strValue = value.deserialize[String]
      Periodicity.byName(strValue).toSuccess(Extractor.Invalid(strValue + " is not a recognized periodicity."))
    }
  }

  implicit val VariableDecomposer = new Decomposer[Variable] {
    def decompose(v: Variable): JValue = v.name match {
      case JPath.Identity => "id"
      case jpath => jpath.serialize
    }
  }

  implicit val VariableExtractor = new Extractor[Variable] {
    def extract(v: JValue): Variable = v match {
      case JString("id") => Variable(JPath.Identity)
      case _ => Variable(v.deserialize[JPath])
    }
  }

  final implicit val HasChildDecomposer = new Decomposer[HasChild] {
    def decompose(v: HasChild): JValue = JObject(
      JField("variable", v.variable.serialize) :: 
      JField("value", v.child.serialize) :: Nil
    )
  }

  final implicit val HasChildExtractor = new Extractor[HasChild] {
    def extract(v: JValue): HasChild = HasChild(
      (v \ "variable").deserialize[Variable],
      (v \ "value").deserialize[JPathNode]
    )
  }

  final implicit val HasValueDecomposer = new Decomposer[HasValue] {
    def decompose(v: HasValue): JValue = JObject(
      JField("variable", v.variable.serialize) :: 
      JField("value", v.value) :: Nil
    )
  }

  final implicit val HasValueExtractor = new Extractor[HasValue] {
    def extract(v: JValue): HasValue = HasValue(
      (v \ "variable").deserialize[Variable],
      (v \ "value")
    )
  }

}

// vim: set ts=4 sw=4 et:
