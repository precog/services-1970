package com.reportgrid.analytics

import blueeyes._
import blueeyes.json.JsonAST._
import blueeyes.json.{JPath, JPathIndex, JPathField}

import com.reportgrid.analytics._
import com.reportgrid.util.MapUtil._

import scalaz.{Ordering => _, _}
import Scalaz._

/** 
 * A report counts observations of a particular type.
 * An observation is a value of type Set[(Variable, HasValue | HasChild)]
 */
case class Report[S <: Predicate, T: Semigroup](observationCounts: Map[Observation[S], T]) {
  /** Creates a new report containing all the data in this report, plus all the
   * data in that report.
   */
  def + (that: Report[S, T]): Report[S, T] = Report[S, T](this.observationCounts <+> that.observationCounts)

  /** Maps the report based on the type of count.
   */
  def map[TT: Semigroup](f: T => TT): Report[S, TT] = Report(observationCounts.mapValues(f))

  /** Groups the report by order of observation.
   */
  def groupByOrder: Map[Int, Report[S, T]] = observationCounts.groupBy(_._1.size).mapValues(Report(_))

  /** Creates a new report derived from this one containing only observations
   * of the specified order.
   */
  def order(n: Int): Report[S, T] = Report(observationCounts.filter(_._1.size == n))
}

object Report {
  def empty[S <: Predicate, T: Semigroup]: Report[S, T] = Report[S, T](Map.empty)

  /** Creates a report of values.
   */
  def ofValues[T: Semigroup](event: JValue, count: T, order: Int, depth: Int, limit: Int): (Report[HasValue, T], Report[HasValue, T]) = {
    // TODO: Change to the following when we move to scala 2.9.0
    // val data = event.flattenWithPath.take(limit)
    // Report(
    //   (for (i <- 1 until order; subset <- data.combinations(i)) yield (subset.toSet, count)).toMap
    // )

    val (infinite, finite) = event.flattenWithPath.take(limit).map {
      case (jpath, jvalue) => (Variable(jpath), HasValue(jvalue))
    } partition {
      case (Variable(jpath), _) => jpath.endsInInfiniteValueSpace
    }
  
    (
      Report(sublists(finite, order).map(subset => (subset.toSet, count))(collection.breakOut)),
      Report(infinite.map(v => (Set(v), count))(collection.breakOut))
    )
  }

  /** Creates a report of children. Although the "order" parameter is supported,
   * it's recommended to always use a order = 1, because higher order counts do
   * not contain much additional information.
   */
  def ofChildren[T: Semigroup](event: JValue, count: T, order: Int, depth: Int, limit: Int): Report[HasChild, T] = {
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
