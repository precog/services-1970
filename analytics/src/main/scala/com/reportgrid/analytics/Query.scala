package com.reportgrid.analytics

sealed trait Term
case class ValueTerm(hasValue: HasValue) extends Term
case class ChildTerm(hasChild: HasChild) extends Term
case class IntervalTerm(interval: Interval) extends Term
case class TimeSpanTerm(span: TimeSpan) extends Term
case class TagMatchTerm(location: Hierarchy.Location)
// vim: set ts=4 sw=4 et:
