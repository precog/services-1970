package com.reportgrid.analytics
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.JsonAST._

import com.reportgrid.analytics.persistence.MongoSupport._
import SortOrder._

sealed trait SortOrder 
object SortOrder {
  implicit val SortOrderExtractor = new Extractor[SortOrder] {
    def extract(jvalue: JValue): SortOrder = jvalue match {
      case JString("Ascending")  => Ascending
      case JString("Descending") => Descending
      
      case _ => error("Invalid sort order: " + jvalue)
    }
  }

  implicit val SortOrderDecomposer = new Decomposer[SortOrder] {
    def decompose(order: SortOrder) = order match {
      case Ascending  => JString("Ascending")
      case Descending => JString("Descending")
    }
  }
}

case object Ascending extends SortOrder
case object Descending extends SortOrder

case class VariableDescriptor(variable: Variable, sortOrder: SortOrder)

object VariableDescriptor {
  implicit val VariableDescriptorExtractor = new Extractor[VariableDescriptor] {
    def extract(jvalue: JValue): VariableDescriptor = VariableDescriptor(
      variable = (jvalue \ "variable").deserialize[Variable],
      sortOrder = (jvalue \ "sortOrder").deserialize[SortOrder]
    )
  }

  implicit val VariableDescriptorDecomposer = new Decomposer[VariableDescriptor] {
    def decompose(descriptor: VariableDescriptor): JValue = JObject(
      JField("variable", descriptor.variable.serialize) ::
      JField("sortOrder", descriptor.sortOrder.serialize) ::
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
