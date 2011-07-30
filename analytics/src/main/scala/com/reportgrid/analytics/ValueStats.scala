package com.reportgrid.analytics

import scalaz.Scalaz._

case class ValueStats(count: Long, sum: Option[Double], sumsq: Option[Double]) {
  lazy val mean = sum.map(_ / count)
  lazy val variance = for (sum <- sum; sumsq <- sumsq) yield IncrementalStatistics.variance(count, sum, sumsq)
  lazy val standardDeviation = variance.map(math.sqrt)
}

object ValueStats {
  implicit val group: AbelianGroup[ValueStats] = new AbelianGroup[ValueStats] {
    override val zero = ValueStats(0, None, None)
    override def inverse(v: ValueStats) = ValueStats(-v.count, v.sum.map(-_), v.sumsq.map(-_))
    override def append(v1: ValueStats, v2: => ValueStats) = {
      ValueStats(v1.count + v2.count, v1.sum |+| v2.sum, v1.sumsq |+| v2.sumsq)
    }
  }
}

case class ValueSet(count: Long, distribution: Map[String, Long])

// vim: set ts=4 sw=4 et:
