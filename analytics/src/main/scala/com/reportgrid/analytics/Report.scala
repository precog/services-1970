package com.reportgrid.analytics

import blueeyes._
import blueeyes.json.JsonAST._
import blueeyes.json.{JPath, JPathIndex, JPathField}

import com.reportgrid.analytics._
import com.reportgrid.util.MapUtil._

import scala.collection.immutable.IndexedSeq
import scalaz.{Ordering => _, _}
import Scalaz._

/** 
 * A report counts observations of a particular type.
 * An observation is a value of type Set[(Variable, HasValue | HasChild)]
 */
case class Report[P <: Predicate, T: Semigroup](observationCounts: Map[Observation[P], T]) {
  /** Creates a new report containing all the data in this report, plus all the
   * data in that report.
   */
  def + (that: Report[P, T]): Report[P, T] = Report[P, T](that.observationCounts <+> this.observationCounts)

  /** Maps the report based on the type of count.
   */
  def map[TT: Semigroup](f: T => TT): Report[P, TT] = Report(observationCounts.mapValues(f))

  /** Groups the report by order of observation.
   */
  def groupByOrder: Map[Int, Report[P, T]] = observationCounts.groupBy(_._1.size).mapValues(Report(_))

  /** Creates a new report derived from this one containing only observations
   * of the specified order.
   */
  def order(n: Int): Report[P, T] = Report(observationCounts.filter(_._1.size == n))
}

object Report {
  def empty[P <: Predicate, T: Semigroup]: Report[P, T] = Report[P, T](Map.empty)

  /** Creates a report of values.
   */
  def ofValues[T: Semigroup](event: JValue, count: T, order: Int, depth: Int, limit: Int): (Report[HasValue, T], Report[HasValue, T]) = {
    val (infinite, finite) = event.flattenWithPath.take(limit).map {
      case (jpath, jvalue) => (Variable(jpath), HasValue(jvalue))
    } partition {
      case (Variable(jpath), _) => jpath.endsInInfiniteValueSpace
    }
  
    (
      power(finite, order, count),
      Report[HasValue, T](infinite.map(v => (Set(v), count))(collection.breakOut))
    )
  }

  /** Creates a report of children. Although the "order" parameter is supported,
   * it's recommended to always use a order = 1, because higher order counts do
   * not contain much additional information.
   */
  def ofChildren[T: Semigroup](event: JValue, count: T, order: Int, depth: Int, limit: Int): Report[HasChild, T] = {
    val flattened = event.foldDownWithPath(Set.empty[(Variable, HasChild)]) { (set, jpath, jvalue) =>
      val parent = jpath.parent
      parent.map(parent => set + (Variable(parent) -> HasChild(jpath.nodes.last))).getOrElse(set)
    }

    power(flattened, order, count)
  }

  def ofInnerNodes[T: Semigroup](event: JValue, count: T, order: Int, depth: Int, limit: Int): Report[HasChild, T] = {
    val flattened = event.foldDownWithPath(Set.empty[(Variable, HasChild)]) { (set, jpath, jvalue) =>
      jvalue match {
        case JNothing | JNull | JBool(_) | JInt(_) | JDouble(_) | JString(_) => set
          // exclude the path when the jvalue indicates a leaf node
        case _ =>
          val parent = jpath.parent
          parent.map(parent => set + (Variable(parent) -> HasChild(jpath.nodes.last))).getOrElse(set)
      }
    }

    power(flattened, order, count)
  }

  private def power[P <: Predicate, T: Semigroup](l: Iterable[(Variable, P)], order: Int, count: T) = {
    Report[P, T]((1 to order).flatMap(l.toSeq.combinations).map(obs => (obs.toSet, count))(collection.breakOut))
  }
}
