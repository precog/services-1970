package com.reportgrid.analytics

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.Printer._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.JodaSerializationImplicits._

import scala.annotation.tailrec

sealed trait Tag {
  def name: String
}

object Tag {
  implicit object TagDecomposer extends Decomposer[Tag] {
    def decompose(v: Tag): JValue = v match {
      case SimpleTag(name, values) => JObject(
        JField("#" + name, values.serialize) :: Nil
      )

      case LODTag(name, values) => JObject(
        JField("#" + name, JArray(
          values map {
            case AnonLODValue(value) => JString(value.mkString("/"))
            case NamedLODValue(name, value) => JObject(JField(name, JString(value.mkString("/"))) :: Nil) 
          }
        )) :: Nil
      )
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
          
          if (rights.isEmpty) {
            LODTag.create(tagName, lefts.map(t => NamedLODValue(t._1, t._2.split("/").toList)))
          } else {
            val values = rights.map(_.split("/").toList)
            if (LODTag.respectsRefinementRule(values)) LODTag.create(tagName, values.map(AnonLODValue(_)))
            else SimpleTag(tagName, rights.toSet)
          }

        case _ => sys.error("Illegal tag format: " + renderNormalized(v))
      }
    }
  }
}

case class SimpleTag(name: String, values: Set[String]) extends Tag
case class LODTag private (name: String, values: List[LODValue]) extends Tag 

object LODTag {
  def create[T <: LODValue](name: String, values: List[T]) = if (respectsRefinementRule(values.map(_.value))) {
    new LODTag(name, values.sortBy(_.value.length))
  } else {
    sys.error("The level-of-detail values specified do not respect the Refinement Rule.")
  }

  def respectsRefinementRule(values: List[List[String]]): Boolean = {
    @tailrec def parallel(l: List[List[String]], acc: Boolean): Boolean = {
      val (heads, tails) = l.foldLeft((List.empty[String], List.empty[List[String]])) {
        case ((heads, tails), x :: xs) => (x :: heads, xs :: tails)
        case (ht, Nil) => ht
      }

      if (tails.isEmpty) acc && heads.distinct.size == 1
      else parallel(tails, acc && heads.distinct.size == 1)
    }

    values.map(_.length).distinct.size == 1 && parallel(values, true)
  }
}

sealed trait LODValue {
  def value: List[String]
}
case class AnonLODValue(value: List[String]) extends LODValue
case class NamedLODValue(name: String, value: List[String]) extends LODValue
