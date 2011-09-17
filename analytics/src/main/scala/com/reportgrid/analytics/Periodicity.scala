package com.reportgrid.analytics

import scala.collection.immutable.NumericRange
import org.joda.time.Instant
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.DateTimeZone.UTC

import scalaz.Scalaz._

sealed trait Periodicity extends Ordered[Periodicity] { self: Product =>
  /** The name of the periodicity, e.g., "hour"
   */
  lazy val name: String = self.productPrefix.toLowerCase

  def byteValue: Byte

  /** Chops off all components of the date time whose periodicities are
   * smaller than this periodicity.
   */
  def floor(time: Instant): Instant

  def ceil(time: Instant): Instant = increment(floor(time))

  /** Advances the date time by this periodicity.
   */
  def increment(time: Instant, amount: Int = 1): Instant

  def period(time: Instant): Period = Period(this, time)

  def indexOf(time: DateTime, in: Periodicity): Option[Int]

  /** The previous periodicity in the chain.
   */
  lazy val previous: Periodicity = previousOption.getOrElse(this)

  lazy val previousOption: Option[Periodicity] = (Periodicity.All.indexOf(this) - 1) match {
    case index: Int if (index < 0) => None
    case index: Int => Some(Periodicity.All(index))
  }

  def finer: Option[Periodicity]

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

object Instants {
  val Zero = new Instant(0)
  val Inf = new Instant(Long.MaxValue)
}

object Periodicity {
  import Instants._

  case object Second extends Periodicity {
    override final val byteValue = 0: Byte

    def floor(time: Instant) = time.toDateTime(UTC).withMillisOfSecond(0).toInstant

    def increment(time: Instant, amount: Int = 1) = time.toDateTime(UTC).plusSeconds(amount).toInstant

    def indexOf(time: DateTime, in: Periodicity): Option[Int] = in match {
      case Minute => Some(time.getSecondOfMinute)
      case Hour   => Some(time.getSecondOfMinute + (60 * time.getMinuteOfHour))
      case Day    => Some(time.getSecondOfDay)
      case _ => None
    }

    override val finer = None
  }

  case object Minute extends Periodicity {
    override final val byteValue = 1: Byte

    def floor(time: Instant) = Second.floor(time).toDateTime(UTC).withSecondOfMinute(0).toInstant

    def increment(time: Instant, amount: Int = 1) = time.toDateTime(UTC).plusMinutes(amount).toInstant

    def indexOf(time: DateTime, in: Periodicity): Option[Int] = in match {
      case Hour => Some(time.getMinuteOfHour)
      case Day  => Some(time.getMinuteOfDay)
      case _ => None
    }

    override val finer = Some(Second)
  }

  case object Hour extends Periodicity {
    override final val byteValue = 2: Byte

    def floor(time: Instant) = Minute.floor(time).toDateTime(UTC).withMinuteOfHour(0).toInstant

    def increment(time: Instant, amount: Int = 1) = time.toDateTime(UTC).plusHours(amount).toInstant

    def indexOf(time: DateTime, in: Periodicity): Option[Int] = in match {
      case Day   => Some(time.getHourOfDay)
      case Week  => Some(time.getHourOfDay + ((time.getDayOfWeek - 1)  * 24))
      case Month => Some(time.getHourOfDay + ((time.getDayOfMonth - 1) * 24))
      case Year  => Some(time.getHourOfDay + ((time.getDayOfYear - 1)  * 24))
      case _ => None
    }

    override val finer = Some(Minute)
  }

  case object Day extends Periodicity {
    override final val byteValue = 3: Byte

    def floor(time: Instant) = Hour.floor(time).toDateTime(UTC).withHourOfDay(0).toInstant

    def increment(time: Instant, amount: Int = 1) = time.toDateTime(UTC).plusDays(amount).toInstant

    def indexOf(time: DateTime, in: Periodicity): Option[Int] = in match {
      case Week =>  Some(time.getDayOfWeek)
      case Month => Some(time.getDayOfMonth)
      case Year =>  Some(time.getDayOfYear)
      case _ => None
    }

    override val finer = Some(Hour)
  }

  case object Week extends Periodicity {
    override final val byteValue = 4: Byte

    def floor(time: Instant) = Day.floor(time).toDateTime(UTC).withDayOfWeek(1).toInstant

    def increment(time: Instant, amount: Int = 1) = time.toDateTime(UTC).plusWeeks(amount).toInstant

    def indexOf(time: DateTime, in: Periodicity): Option[Int] = in match {
      case Year => Some(time.getWeekOfWeekyear)
      case _ => None
    }

    override val finer = Some(Day)
  }

  case object Month extends Periodicity {
    override final val byteValue = 5: Byte

    def floor(time: Instant) = Day.floor(time).toDateTime(UTC).withDayOfMonth(1).toInstant

    def increment(time: Instant, amount: Int = 1) = time.toDateTime(UTC).plusMonths(amount).toInstant

    def indexOf(time: DateTime, in: Periodicity): Option[Int] = in match {
      case Year => Some(time.getMonthOfYear)
      case _ => None
    }

    override val finer = Some(Day)
  }

  case object Year extends Periodicity {
    override final val byteValue = 6: Byte

    def floor(time: Instant) = Month.floor(time).toDateTime(UTC).withMonthOfYear(1).toInstant

    def increment(time: Instant, amount: Int = 1) = time.toDateTime(UTC).plusYears(amount).toInstant

    def indexOf(time: DateTime, in: Periodicity): Option[Int] = None

    override val finer = Some(Month)
  }

  case object Eternity extends Periodicity {
    override final val byteValue = Byte.MaxValue

    def floor(time: Instant) = Instants.Zero

    def increment(time: Instant, amount: Int = 1) = Instants.Inf

    def indexOf(time: DateTime, in: Periodicity): Option[Int] = None

    override val finer = Some(Year)
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

  def byName(name: String): Option[Periodicity] = All.find(_.name == name.toLowerCase)

  def apply(name: String): Periodicity = byName(name).getOrElse(sys.error("Invalid periodicity name: " + name))

  def unapply(periodicity: Periodicity): Option[String] = Some(periodicity.name)
}
