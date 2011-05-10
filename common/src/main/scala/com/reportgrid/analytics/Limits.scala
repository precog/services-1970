package com.reportgrid.analytics
import blueeyes.json.xschema._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.DefaultSerialization._

import scala.math.{min}

/** Limits represent hard limits on the data aggregated by ReportGrid. They
 * are typically set at a customer-by-customer basis, depending on the plan
 * the customer signs up for. Higher limits imply more power in aggregation
 * but more resource utilization on the backend.
 */
case class Limits(order: Int, limit: Int, depth: Int) {
  def limitTo(that: Limits) = Limits(
    order = min(this.order, that.order),
    limit = min(this.limit, that.limit),
    depth = min(this.limit, that.limit)
  )
}

object Limits {
  val None = Limits(100, 100, 100)

  implicit val LimitsExtractor = new Extractor[Limits] {
    def extract(jvalue: JValue): Limits = Limits(
      order = (jvalue \ "order").deserialize[Int],
      limit = (jvalue \ "limit").deserialize[Int],
      depth = (jvalue \ "depth").deserialize[Int]
    )
  }

  implicit val LimitsDecomposer = new Decomposer[Limits] {
    def decompose(limits: Limits): JValue = JObject(
      JField("order", limits.order.serialize) ::
      JField("limit", limits.order.serialize) ::
      JField("depth", limits.order.serialize) ::
      Nil
    )
  }

}
