package com.reportgrid
package analytics

import scalaz.Scalaz._

case class ValueStats(count: Long, sum: Option[Double], sumsq: Option[Double]) {
  lazy val mean = sum.map(_ / count)
  lazy val variance = for (sum <- sum; sumsq <- sumsq) yield IncrementalStatistics.variance(count, sum, sumsq)
  lazy val standardDeviation = variance.map(math.sqrt)
}

object ValueStats {
  trait Group extends AbelianGroup[ValueStats] {
    override val zero = ValueStats(0, None, None)
    override def inverse(v: ValueStats) = ValueStats(-v.count, v.sum.map(-_), v.sumsq.map(-_))
    override def append(v1: ValueStats, v2: => ValueStats) = {
      ValueStats(v1.count + v2.count, v1.sum |+| v2.sum, v1.sumsq |+| v2.sumsq)
    }
  }

  trait VSInterpolator extends Interpolator[ValueStats] {
    def interpolate(a: ValueStats, b: ValueStats, c: ValueStats, d: ValueStats, x: Double): ValueStats = {
      import Interpolator._
      ValueStats(
        Interpolator.interpolate(a.count, b.count, c.count, d.count, x),
        (a.sum |@| b.sum |@| c.sum |@| d.sum) { Interpolator.interpolate(_, _, _, _, x) },
        (a.sumsq |@| b.sumsq |@| c.sumsq |@| d.sumsq) { Interpolator.interpolate(_, _, _, _, x) }
      )
    }
  }

  implicit object ValueStatsMDouble extends ct.Mult.MDouble[ValueStats] {
    override def append(a: ValueStats, b: => Double) = ValueStats(
      count = (a.count * b).round,
      sum = a.sum.map(_ * b),
      sumsq = a.sumsq.map(_ * b)
    )
  }

  implicit object Ops extends Group with VSInterpolator
}

trait Interpolator[A] {
  def interpolate(a: A, b: A, c: A, d: A, x: Double): A
}

object Interpolator {
  def interpolate[A: Interpolator](a: A, b: A, c: A, d: A, x: Double): A = implicitly[Interpolator[A]].interpolate(a,b,c,d,x)

  implicit val DoubleInterpolator: Interpolator[Double] = new Interpolator[Double] {
    def interpolate(a: Double, b: Double, c: Double, d: Double, x: Double): Double = {
      b + 0.5 * x*(c - a + x*(2.0*a - 5.0*b + 4.0*c - d + x*(3.0*(b - c) + d - a)))
    }
  }

  implicit val LongInterpolator: Interpolator[Long] = new Interpolator[Long] {
    def interpolate(a: Long, b: Long, c: Long, d: Long, x: Double): Long = {
      (b + 0.5 * x*(c - a + x*(2.0*a - 5.0*b + 4.0*c - d + x*(3.0*(b - c) + d - a)))).toLong
    }
  }
}
