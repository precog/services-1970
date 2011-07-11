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

  implicit def ReportAggregator[S <: Predicate, T: AbelianGroup] = new AbelianGroup[Report[S, T]] {
    private val aggT = implicitly[AbelianGroup[T]]

    val zero = Report.empty[S, T]

    def inverse(v: Report[S, T]): Report[S, T] = Report(v.observationCounts.transform { (k, v) => aggT.inverse(v) })

    def append(v1: Report[S, T], v2: => Report[S, T]) = v1 + v2
  }

  implicit def optionGroup[A](implicit ga: AbelianGroup[A]): AbelianGroup[Option[A]] = new AbelianGroup[Option[A]] {
    val zero: Option[A] = None
    def inverse(o: Option[A]) = o.map(ga.inverse)
    def append(o1: Option[A], o2: => Option[A]) = o1.map(a1 => o2.map(_ |+| a1).getOrElse(a1)).orElse(o2)
  }
 
//
//  implicit def mapAggregator[K, V](implicit gv: AbelianGroup[V]): AbelianGroup[Map[K, V]] = new AbelianGroup[Map[K, V]] {
//    val zero = Map[K, V]()
//
//    def inverse(v: Map[K, V]): Map[K, V] = v.mapValues(gv.inverse)
//
//    def append(t1: Map[K, V], t2: => Map[K, V]): Map[K, V] = {
//      val keys = t1.keys ++ t2.keys
//
//      keys.foldLeft(Map[K, V]()) { (map, key) =>
//        val value1 = t1.get(key).getOrElse(gv.zero)
//        val value2 = t2.get(key).getOrElse(gv.zero)
//
//        map + (key -> gv.append(value1, value2))
//      }
//    }
//  }

  implicit val valueStatsGroup = ValueStats.group
}

object AggregatorImplicits extends AggregatorImplicits
