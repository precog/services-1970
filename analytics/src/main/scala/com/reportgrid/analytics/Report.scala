package com.reportgrid.analytics

import blueeyes._
import blueeyes.json.JsonAST._
import blueeyes.json.{JPath, JPathIndex, JPathField}

import com.reportgrid.util.MapUtil._

import scalaz.{Ordering => _, _}
import Scalaz._

/** 
 * A report counts observations of a particular type.
 * An observation is a value of type Set[(Variable, HasValue | HasChild)]
 */
case class Report[S <: Predicate, T: AbelianGroup](observationCounts: Map[Observation[S], T]) {
  private val aggregator: AbelianGroup[T] = implicitly[AbelianGroup[T]]

  /** Creates a new report containing all the data in this report, plus all the
   * data in that report.
   */
  def + (that: Report[S, T]): Report[S, T] = Report[S, T](this.observationCounts <+> that.observationCounts)

  /** Maps the report based on the type of count.
   */
  def map[TT: AbelianGroup](f: T => TT): Report[S, TT] = Report(observationCounts.mapValues(f))

  /** Groups the report by order of observation.
   */
  def groupByOrder: Map[Int, Report[S, T]] = observationCounts.groupBy(_._1.size).mapValues(Report(_))

  /** Creates a new report derived from this one containing only observations
   * of the specified order.
   */
  def order(n: Int): Report[S, T] = Report(observationCounts.filter(_._1.size == n))

  /** Groups the report by period, for a time-series report (or one that's
   * isomorphic to a time series report).
   */
  def groupByPeriod[V](implicit witness: T => TimeSeries[V], group: AbelianGroup[V]): Map[Period, Report[S, TimeSeries[V]]] = {
    flip(observationCounts.mapValues(witness(_).groupByPeriod)).mapValues(Report(_))
  }

  def groupByPeriodicity[V](implicit witness: T => TimeSeries[V], group: AbelianGroup[V]): Map[Periodicity, Report[S, TimeSeries[V]]] = {
    flip(observationCounts.mapValues(witness(_).groupByPeriodicity)).mapValues(Report(_))
  }

  /**
   * Convert this report to a map of reports from period to time series report. In the resulting
   * map, the keys will be periods that batch reports of finer granularity.
   */
  def partition[V](pf: Periodicity => Periodicity)(implicit witness: T => TimeSeries[V], group: AbelianGroup[V]): Map[BatchKey[S], TimeSeries[V]] = {
    observationCounts.mapValues(witness).foldLeft(Map.empty[BatchKey[S], TimeSeries[V]]) {
      case (m, (observation, timeSeries)) => timeSeries.series.foldLeft(m) {
        case (m, entry @ (p, v)) => 
          val batchKey = BatchKey(pf(p.periodicity).period(p.start), p.periodicity, observation)
          m + (batchKey -> (m.getOrElse(batchKey, TimeSeries.empty[V]) + entry))
      }
    }
  }
}

case class BatchKey[S <: Predicate](period: Period, valuePeriodicity: Periodicity, observation: Observation[S])

object Report {
  def empty[S <: Predicate, T: AbelianGroup]: Report[S, T] = Report[S, T](Map.empty)

  /** Creates a report of values.
   */
  def ofValues[T: AbelianGroup](event: JValue, count: T, order: Int, depth: Int, limit: Int): Report[HasValue, T] = {
    // TODO: Change to the following when we move to scala 2.9.0
    // val data = event.flattenWithPath.take(limit)
    // Report(
    //   (for (i <- 1 until order; subset <- data.combinations(i)) yield (subset.toSet, count)).toMap
    // )

    val flattened = event.flattenWithPath.take(limit).map {
      case (jpath, jvalue) => (Variable(jpath), HasValue(jvalue))
    }
  
    /*def factorial(n: Int) : Long = {
      def factorial0(n: Int, acc: Long): Long = {
        if (n <= 1) acc
        else factorial0(n - 1, acc * n)
      }

      factorial0(n, 1L)
    }

    def nchoosek(n: Int, k: Int): Long = factorial(n) / (factorial(k) * factorial(n - k))

    val ss = sublists(flattened, order)

    println("actual length: " + ss.length + ", expected length: " + (1 to order).foldLeft(0L)((length, order) => length + nchoosek(flattened.length, order)))*/

    Report(Map(sublists(flattened, order).map(subset => (subset.toSet, count)): _*))
  }

  /** Creates a report of children. Although the "order" parameter is supported,
   * it's recommended to always use a order = 1, because higher order counts do
   * not contain much additional information.
   */
  def ofChildren[T: AbelianGroup](event: JValue, count: T, order: Int, depth: Int, limit: Int): Report[HasChild, T] = {
    val agg = implicitly[AbelianGroup[T]]

    val empty = Set.empty[(Variable, HasChild)]

    val flattened = event.foldDownWithPath(empty) { (set, jpath, jvalue) =>
      val parent = jpath.parent

      parent.map { parent =>
        val child  = jpath.nodes.last

        set + (Variable(parent) -> HasChild(child))
      }.getOrElse(set)
    }.toList

    Report(Map(sublists(flattened, order).map { subset => (subset.toSet, count) }: _*))
  }
}
