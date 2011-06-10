package com.reportgrid.analytics

import scala.collection.immutable.NumericRange

import org.joda.time.{DateTime, DateTimeZone}

/** A globally unique identifier for a particular period in time (second,
 * minute, hour, day, week, month, year, or eternity).
 */
case class Period private (periodicity: Periodicity, start: DateTime, end: DateTime) extends Ordered[Period] {
  /** Compares this id and another based first on periodicity, and second on index.
   */
  def compare(that: Period) = (this.periodicity.compare(that.periodicity) :: this.start.getMillis.compare(that.start.getMillis) :: Nil).dropWhile(_ == 0).headOption.getOrElse(0)

  def withPeriodicity(p: Periodicity): Period = Period(p, start)

  def to(that: Period): Stream[Period] = {
    import Stream.{cons, empty}

    ((that: Period) => {
      val c = this.start.getMillis - that.start.getMillis

      if (c > 0) empty
      else if (c == 0) cons(this, empty)
      else {
        val next = Period(periodicity, periodicity.increment(start), periodicity.increment(end))

        cons(next, next.to(that))
      }
    })(that.withPeriodicity(periodicity))
  }

  def until(that: Period): Stream[Period] = {
    import Stream.{cons, empty}

    val c = this.start.getMillis - that.start.getMillis

    if (c >= 0) empty
    else {
      val next = Period(periodicity, periodicity.increment(start), periodicity.increment(end))

      cons(next, next.to(that))
    }
  }
}

object Period {
  val Eternity = Periodicity.Eternity.period(Periodicity.Zero)

  /** Constructs a period from a periodicity and any time occurring within the
   * period.
   */
  def apply(periodicity: Periodicity, start: DateTime): Period = {
    val flooredStart = periodicity.floor(start)

    new Period(periodicity, flooredStart, periodicity.increment(flooredStart))
  }
}
