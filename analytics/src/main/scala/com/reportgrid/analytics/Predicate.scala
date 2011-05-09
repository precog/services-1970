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