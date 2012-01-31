package com.reportgrid.analytics

import scala.collection.immutable.NumericRange

import org.joda.time.{Instant, Duration}
import scalaz.Scalaz._

/** A globally unique identifier for a particular period in time (second,
 * minute, hour, day, week, month, year, or eternity).
 */
sealed class Period private (val periodicity: Periodicity, val start: Instant) extends Ordered[Period] {
  /** Compares this id and another based first on periodicity, and second on index.
   */
  def compare(that: Period) = (this.periodicity.compare(that.periodicity) :: this.start.getMillis.compare(that.start.getMillis) :: Nil).dropWhile(_ == 0).headOption.getOrElse(0)

  /** The size of the period.
   */
  def size: Duration = new Duration(start, end)

  def contains(time: Instant): Boolean = time.getMillis >= start.getMillis && time.getMillis < end.getMillis

  def withPeriodicity(p: Periodicity): Period = Period(p, start)

  lazy val end = periodicity.increment(start)

  lazy val timeSpan = TimeSpan(start, end)

  /** The next period of this periodicity.
    */
  def next: Period = Period(periodicity, periodicity.increment(start))

  def to(that: Instant): Stream[Period] = {
    import Stream.{cons, empty}

    if (this.periodicity == Periodicity.Eternity) cons(Period.Eternity, empty)
    else if (this.start.getMillis > that.getMillis) empty
    else cons(this, next.to(that))
  }

  def datesTo(that: Instant): Stream[Instant] = to(that).map(_.start)

  def until(that: Instant): Stream[Period] = {
    val s = to(that)
    if (s.headOption.isEmpty) Stream.Empty else s.init
  }

  def datesUntil(that: Instant): Stream[Instant] = {
    val s = datesTo(that)
    if (s.headOption.isEmpty) Stream.Empty else s.init
  }

  override def equals(that: Any) = that match {
    case that @ Period(`periodicity`, _, _) => this.start.getMillis == that.start.getMillis
    case _ => false
  }

  override def hashCode: Int = periodicity.hashCode | start.getMillis.hashCode

  override def toString = "Period(" + periodicity + "," + start + ")"
}

object Period {
  val Eternity = apply(Periodicity.Eternity, Instants.Zero)

  /** Constructs a period from a periodicity and any time occurring within the
   * period.
   */
  def apply(periodicity: Periodicity, start: Instant): Period = {
    new Period(periodicity, periodicity.floor(start))
  }

  def unapply(p: Period): Option[(Periodicity, Instant, Instant)] = Some((p.periodicity, p.start, p.end))
}
