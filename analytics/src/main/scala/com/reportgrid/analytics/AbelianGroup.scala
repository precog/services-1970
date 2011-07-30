package com.reportgrid.analytics

import scalaz._
import Scalaz._

trait AbelianGroup[T] extends Monoid[T] {
  def inverse(v: T): T
}

trait AggregatorImplicits {
  implicit val intAggregator = new AbelianGroup[Int] {
    val zero = 0

    def inverse(v: Int): Int = -v

    def append(t1: Int, t2: => Int): Int = t1 + t2
  }

  implicit val longAggregator = new AbelianGroup[Long] {
    val zero = 0L

    def inverse(v: Long): Long = -v

    def append(t1: Long, t2: => Long): Long = t1 + t2
  }

  implicit val floatAggregator = new AbelianGroup[Float] {
    val zero = 0.0F

    def inverse(v: Float): Float = -v

    def append(t1: Float, t2: => Float): Float = t1 + t2
  }

  implicit val doubleAggregator = new AbelianGroup[Double] {
    val zero = 0.0

    def inverse(v: Double): Double = -v

    def append(t1: Double, t2: => Double): Double = t1 + t2
  }

  implicit def optionGroup[A](implicit ga: AbelianGroup[A]): AbelianGroup[Option[A]] = new AbelianGroup[Option[A]] {
    val zero: Option[A] = None
    def inverse(o: Option[A]) = o.map(ga.inverse)
    def append(o1: Option[A], o2: => Option[A]) = o1.map(a1 => o2.map(_ |+| a1).getOrElse(a1)).orElse(o2)
  }
 
  implicit val valueStatsGroup = ValueStats.group
}

object AggregatorImplicits extends AggregatorImplicits
