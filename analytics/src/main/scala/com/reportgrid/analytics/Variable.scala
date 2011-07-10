package com.reportgrid.analytics

import blueeyes.json.JPath
import scalaz.Scalaz._

case class Variable(name: JPath) {
  def parent: Option[Variable] = name.parent.map { parent => copy(name = parent) }
}

object Variable {
  implicit val orderingVariable: Ordering[Variable] = new Ordering[Variable] {
    override def compare(v1: Variable, v2: Variable) = {
      v1.name.toString.compare(v2.name.toString)
    }
  }
}

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
      ValueStats(v1.count + v2.count, v1.sum <+> v2.sum, v1.sumsq <+> v2.sumsq)
    }
  }
}

case class ValueSet(count: Long, distribution: Map[String, Long])
