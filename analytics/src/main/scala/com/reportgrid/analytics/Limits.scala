package com.reportgrid.analytics

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
}