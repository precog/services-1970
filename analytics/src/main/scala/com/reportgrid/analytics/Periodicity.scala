package com.reportgrid.analytics

import scala.collection.immutable.NumericRange

import org.joda.time.Instant

sealed trait Periodicity extends Ordered[Periodicity] { self: Product =>
  /** The name of the periodicity, e.g., "hour"
   */
  lazy val name: String = self.productPrefix.toLowerCase

  /** Chops off all components of the date time whose periodicities are
   * smaller than this periodicity.
   */
  def floor(time: Instant): Instant

  def ceil(time: Instant): Instant = increment(floor(time))

  /** Advances the date time by this periodicity.
   */
  def increment(time: Instant, amount: Int = 1): Instant

  def period(time: Instant): Period = Period(this, time)

  /** The previous periodicity in the chain.
   */
  lazy val previous: Periodicity = previousOption.getOrElse(this)

  lazy val previousOption: Option[Periodicity] = (Periodicity.All.indexOf(this) - 1) match {
    case index: Int if (index < 0) => None
    case index: Int => Some(Periodicity.All(index))
  }

  /** The next periodicity in the chain.
   */
  lazy val next: Periodicity = nextOption.getOrElse(this)

  lazy val nextOption: Option[Periodicity] = (Periodicity.All.indexOf(this) + 1) match {
    case index: Int if (index == Periodicity.All.length) => None
    case index: Int => Some(Periodicity.All(index))
  }

  /** Returns a list of all periodicities from this one up to and including
   * that one.
   */
  def to(that: Periodicity): List[Periodicity] = (Periodicity.All.indexOf(this) to Periodicity.All.indexOf(that)).map(Periodicity.All.apply _)(collection.breakOut)

  /** Returns a list of all periodicities from this one up to but not including
   * that one.
   */
  def until(that: Periodicity): List[Periodicity] = (Periodicity.All.indexOf(this) until Periodicity.All.indexOf(that)).map(Periodicity.All.apply _)(collection.breakOut)

  /** Compares this periodicity to that periodicity based on length.
   */
  def compare(that: Periodicity): Int = Periodicity.All.indexOf(this).compare(Periodicity.All.indexOf(that))
}

object Periodicity {
  private[analytics] val Zero = new Instant(0)

  private[analytics] val Inf = new Instant(Long.MaxValue)

  case object Second extends Periodicity {
    def floor(time: Instant) = time.toDateTime.withMillisOfSecond(0).toInstant

    def increment(time: Instant, amount: Int = 1) = time.toDateTime.plusSeconds(amount).toInstant
  }

  case object Minute extends Periodicity {
    def floor(time: Instant) = Second.floor(time).toDateTime.withSecondOfMinute(0).toInstant

    def increment(time: Instant, amount: Int = 1) = time.toDateTime.plusMinutes(amount).toInstant
  }

  case object Hour extends Periodicity {
    def floor(time: Instant) = Minute.floor(time).toDateTime.withMinuteOfHour(0).toInstant

    def increment(time: Instant, amount: Int = 1) = time.toDateTime.plusHours(amount).toInstant
  }

  case object Day extends Periodicity {
    def floor(time: Instant) = Hour.floor(time).toDateTime.withHourOfDay(0).toInstant

    def increment(time: Instant, amount: Int = 1) = time.toDateTime.plusDays(amount).toInstant
  }

  case object Week extends Periodicity {
    def floor(time: Instant) = Day.floor(time).toDateTime.withDayOfWeek(1).toInstant

    def increment(time: Instant, amount: Int = 1) = time.toDateTime.plusWeeks(amount).toInstant
  }

  case object Month extends Periodicity {
    def floor(time: Instant) = Day.floor(time).toDateTime.withDayOfMonth(1).toInstant

    def increment(time: Instant, amount: Int = 1) = time.toDateTime.plusMonths(amount).toInstant
  }

  case object Year extends Periodicity {
    def floor(time: Instant) = Month.floor(time).toDateTime.withMonthOfYear(1).toInstant

    def increment(time: Instant, amount: Int = 1) = time.toDateTime.plusYears(amount).toInstant
  }

  case object Eternity extends Periodicity {
    def floor(time: Instant) = Zero

    def increment(time: Instant, amount: Int = 1) = Inf
  }

  val All = Second   ::
            Minute   ::
            Hour     ::
            Day      ::
            Week     ::
            Month    ::
            Year     ::
            Eternity ::
            Nil

  val Default = Periodicity.Minute to Periodicity.Eternity

  def byName(name: String): Periodicity = All.find(_.name == name.toLowerCase).getOrElse(error("Invalid periodicity name: " + name))

  def apply(name: String): Periodicity = byName(name)

  def unapply(periodicity: Periodicity): Option[String] = Some(periodicity.name)
}
