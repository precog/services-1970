package com.reportgrid.analytics

import blueeyes.json.JsonAST._
import blueeyes.json.JPathNode

/** A predicate that holds true of a given variable.
 */
sealed trait Predicate

/** The variable has been observed to take on the specified value.
 */
case class HasValue(value: JValue) extends Predicate

/** The variable has been observed to have the specified child.
 */
case class HasChild(child: JPathNode) extends Predicate

object Obs {
  def ofValue(variable: Variable, value: JValue) = Set(variable -> HasValue(value))
  def ofChild(variable: Variable, child: JPathNode) = Set(variable -> HasChild(child))
}

// vim: set ts=4 sw=4 et:
