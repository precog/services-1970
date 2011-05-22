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
      case Ascending  => JString("Ascending")
      case Descending => JString("Descending")
    }
  }
}

case class VariableDescriptor(variable: Variable, maxResults: Int, sortOrder: SortOrder)

object VariableDescriptor {
  implicit val VariableDescriptorExtractor = new Extractor[VariableDescriptor] {
    def extract(jvalue: JValue): VariableDescriptor = VariableDescriptor(
      variable   = (jvalue \ "property").deserialize[Variable],
      maxResults = (jvalue \ "limit").deserialize[Int],
      sortOrder  = (jvalue \ "order").deserialize[SortOrder]
    )
  }

  implicit val VariableDescriptorDecomposer = new Decomposer[VariableDescriptor] {
    def decompose(descriptor: VariableDescriptor): JValue = JObject(
      JField("property",    descriptor.variable.serialize)   ::
      JField("limit",       descriptor.maxResults.serialize) ::
      JField("order",       descriptor.sortOrder.serialize)  ::
      Nil
    )
  }
}

sealed trait Selection
case object Count extends Selection
case class Series(periodicity: Periodicity) extends Selection

object Selection {
  def apply(select: String) = select match {
    case "count" => Count
    case _ => select.split("/").toList.map(_.toLowerCase) match {
      case "series" :: p :: Nil => Series(Periodicity(p))

      case _ => error("Invalid series")
    }
  }
}
// vim: set ts=4 sw=4 et:
