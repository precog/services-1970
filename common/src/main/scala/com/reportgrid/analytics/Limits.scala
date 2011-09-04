package com.reportgrid.analytics

/** Limits represent hard limits on the data aggregated by ReportGrid. They
 * are typically set at a customer-by-customer basis, depending on the plan
 * the customer signs up for. Higher limits imply more power in aggregation
 * but more resource utilization on the backend.
 */
case class Limits(order: Int, limit: Int, depth: Int, tags: Int = 1) {
  def limitTo(that: Limits) = Limits(
    order = this.order.min(that.order),
    limit = this.limit.min(that.limit),
    depth = this.depth.min(that.depth),
    tags  = this.tags.min(that.tags)
  )
}

object Limits {
  val None = Limits(100, 100, 100)
}
