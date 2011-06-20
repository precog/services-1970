package com.reportgrid.analytics

import scala.collection.immutable.NumericRange

import org.joda.time.{DateTime, DateTimeZone, Duration}

/** A globally unique identifier for a particular period in time (second,
 * minute, hour, day, week, month, year, or eternity).
 */
class Period private (val periodicity: Periodicity, _start: DateTime) extends Ordered[Period] {
  /** Compares this id and another based first on periodicity, and second on index.
   */
  def compare(that: Period) = (this.periodicity.compare(that.periodicity) :: this.start.getMillis.compare(that.start.getMillis) :: Nil).dropWhile(_ == 0).headOption.getOrElse(0)

  /** The size of the period.
   */
  def size: Duration = new Duration(start, end)

  def contains(time: DateTime): Boolean = time.getMillis >= start.getMillis && time.getMillis < end.getMillis

  def withPeriodicity(p: Periodicity): Period = Period(p, start)

  def start = periodicity.floor(_start)

  def end = periodicity.increment(start)

  /** The next period of this periodicity.
    */
  def next: Period = Period(periodicity, periodicity.increment(start))

  def to(that: DateTime): Stream[Period] = {
    import Stream.{cons, empty}

    if (this.start.getMillis > that.getMillis) empty
    else cons(this, next.to(that))
  }

  def until(that: DateTime): Stream[Period] = {
    val s = to(that)

    if (s.headOption.isEmpty) Stream.empty
    else s.init
  }

  override def equals(that: Any) = that match {
    case that @ Period(`periodicity`, _, _) => this.start.getMillis == that.start.getMillis
    case _ => false
  }

  override def hashCode: Int = periodicity.hashCode | start.getMillis.hashCode

  override def toString = "Period(" + periodicity + "," + start + ")"
}

object Period {
  val Eternity = Periodicity.Eternity.period(Periodicity.Zero)

  /** Constructs a period from a periodicity and any time occurring within the
   * period.
   */
  def apply(periodicity: Periodicity, start: DateTime): Period = {
    val flooredStart = periodicity.floor(start)

    new Period(periodicity, flooredStart)
  }

  def unapply(p: Period): Option[(Periodicity, DateTime, DateTime)] = Some((p.periodicity, p.start, p.end))
}
