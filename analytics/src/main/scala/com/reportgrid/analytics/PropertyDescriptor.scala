package com.reportgrid.analytics
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.JsonAST._

import SortOrder._

sealed trait SortOrder 
object SortOrder {
  implicit val SortOrderExtractor = new Extractor[SortOrder] {
    def extract(jvalue: JValue): SortOrder = jvalue match {
      case JString("Ascending")  => Ascending
      case JString("Descending") => Descending
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

case class PropertyDescriptor(path: Path, limit: Int, order: SortOrder)

object PropertyDescriptor {
  implicit val PropertyDescriptorExtractor = new Extractor[PropertyDescriptor] {
    def extract(jvalue: JValue): PropertyDescriptor = PropertyDescriptor(
      path = (jvalue \ "path").deserialize[Path],
      limit = (jvalue \ "limit").deserialize[Int],
      order = (jvalue \ "order").deserialize[SortOrder]
    )
  }

  implicit val PropertyDescriptorDecomposer = new Decomposer[PropertyDescriptor] {
    def decompose(descriptor: PropertyDescriptor): JValue = JObject(
      JField("path", descriptor.path.serialize) ::
      JField("limit", descriptor.limit.serialize) ::
      JField("order", descriptor.order.serialize) ::
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
    }
  }
}
// vim: set ts=4 sw=4 et:
