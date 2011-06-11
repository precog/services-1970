package com.reportgrid.analytics

import scala.collection.immutable.NumericRange

import org.joda.time.{DateTime, DateTimeZone}

sealed trait Periodicity extends Ordered[Periodicity] { self: Product =>
  /** The name of the periodicity, e.g., "hour"
   */
  def name: String = self.productPrefix.toLowerCase

  /** Chops off all components of the date time whose periodicities are
   * smaller than this periodicity.
   */
  def floor(time: DateTime): DateTime

  /** Advances the date time by this periodicity.
   */
  def increment(time: DateTime, amount: Int = 1): DateTime

  def period(time: DateTime): Period = Period(this, floor(time))

  /** The previous periodicity in the chain.
   */
  def previous: Periodicity = previousOption.getOrElse(this)

  def previousOption: Option[Periodicity] = (Periodicity.All.indexOf(this) - 1) match {
    case index: Int if (index < 0) => None
    case index: Int => Some(Periodicity.All(index))
  }

  /** The next periodicity in the chain.
   */
  def next: Periodicity = nextOption.getOrElse(this)

  def nextOption: Option[Periodicity] = (Periodicity.All.indexOf(this) + 1) match {
    case index: Int if (index == Periodicity.All.length) => None
    case index: Int => Some(Periodicity.All(index))
  }

  /** Returns a list of all periodicities from this one up to and including
   * that one.
   */
  def to(that: Periodicity): List[Periodicity] = (Periodicity.All.indexOf(this) to Periodicity.All.indexOf(that)).map(Periodicity.All.apply _).toList

  /** Returns a list of all periodicities from this one up to but not including
   * that one.
   */
  def until(that: Periodicity): List[Periodicity] = (Periodicity.All.indexOf(this) until Periodicity.All.indexOf(that)).map(Periodicity.All.apply _).toList

  /** Compares this periodicity to that periodicity based on length.
   */
  def compare(that: Periodicity): Int = Periodicity.All.indexOf(this).compare(Periodicity.All.indexOf(that))

  override def equals(that: Any): Boolean = that match {
    case that: Periodicity => this.name == that.name

    case _ => false
  }

  override def hashCode = name.hashCode

  override def toString = self.productPrefix.toLowerCase
}

object Periodicity {
  private[analytics] lazy val Zero = new DateTime(0, DateTimeZone.UTC)

  private[analytics] lazy val Inf = new DateTime(Long.MaxValue, DateTimeZone.UTC)

  case object Second extends Periodicity {
    def floor(time: DateTime) = time.withMillisOfSecond(0)

    def increment(time: DateTime, amount: Int = 1) = time.plusSeconds(amount)
  }

  case object Minute extends Periodicity {
    def floor(time: DateTime) = Second.floor(time).withSecondOfMinute(0)

    def increment(time: DateTime, amount: Int = 1) = time.plusMinutes(amount)
  }

  case object Hour extends Periodicity {
    def floor(time: DateTime) = Minute.floor(time).withMinuteOfHour(0)

    def increment(time: DateTime, amount: Int = 1) = time.plusHours(amount)
  }

  case object Day extends Periodicity {
    def floor(time: DateTime) = Hour.floor(time).withHourOfDay(0)

    def increment(time: DateTime, amount: Int = 1) = time.plusDays(amount)
  }

  case object Week extends Periodicity {
    def floor(time: DateTime) = Day.floor(time).withDayOfWeek(1)

    def increment(time: DateTime, amount: Int = 1) = time.plusWeeks(amount)
  }

  case object Month extends Periodicity {
    def floor(time: DateTime) = Week.floor(time).withDayOfMonth(1)

    def increment(time: DateTime, amount: Int = 1) = time.plusMonths(amount)
  }

  case object Year extends Periodicity {
    def floor(time: DateTime) = Month.floor(time).withMonthOfYear(1)

    def increment(time: DateTime, amount: Int = 1) = time.plusYears(amount)
  }

  case object Eternity extends Periodicity {
    def floor(time: DateTime) = Zero

    def increment(time: DateTime, amount: Int = 1) = Inf
  }

  lazy val All = Second   ::
                 Minute   ::
                 Hour     ::
                 Day      ::
                 Week     ::
                 Month    ::
                 Year     ::
                 Eternity ::
                 Nil

  lazy val Default = Periodicity.Minute to Periodicity.Eternity

  def byName(name: String): Periodicity = All.find(_.name == name.toLowerCase).getOrElse(error("Invalid periodicity name: " + name))

  def apply(name: String): Periodicity = byName(name)

  def unapply(periodicity: Periodicity): Option[String] = Some(periodicity.name)
}
