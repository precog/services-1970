package com.reportgrid.analytics
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.JsonAST._

import com.reportgrid.analytics.persistence.MongoSupport._
import SortOrder._

sealed trait SortOrder 
object SortOrder {
  case object Ascending extends SortOrder
  case object Descending extends SortOrder

  implicit val SortOrderExtractor = new Extractor[SortOrder] {
    def extract(jvalue: JValue): SortOrder = jvalue.deserialize[String].toLowerCase match {
      case "ascending"  => Ascending
      case "descending" => Descending
    }
  }

  implicit val SortOrderDecomposer = new Decomposer[SortOrder] {
    def decompose(order: SortOrder) = order match {
      case Ascending  => JString("ascending")
      case Descending => JString("descending")
    }
  }
}

case class VariableDescriptor(variable: Variable, maxResults: Int, sortOrder: SortOrder)

sealed trait Selection
case object Related extends Selection
case object Count extends Selection
case class Series(periodicity: Periodicity) extends Selection

object Selection {
  def apply(select: String) = select match {
    case "related" => Related
    case "count" => Count
    case _ => select.split("/").toList.map(_.toLowerCase) match {
      case "series" :: p :: Nil => Series(Periodicity(p))

      case _ => sys.error("Invalid series")
    }
  }
}
// vim: set ts=4 sw=4 et:
