package com.reportgrid.analytics

import org.joda.time.{Instant, Duration}
import org.specs.{Specification, ScalaCheck}
import org.specs.specification.PendingUntilFixed
import org.scalacheck._
import org.scalacheck.Gen._
import org.scalacheck.Prop._
import Periodicity._

import scalaz.Scalaz._

class PeriodSpec extends Specification with ArbitraryTime with ScalaCheck {
  val genTimeClock = blueeyes.util.Clock.System

  implicit val arbTuple = Arbitrary {
    for {
      periodicity <- genPeriodicity(Minute, Hour)
      start       <- genPeriod(periodicity)
      end         <- genTime

      if (start.start.getMillis <= end.getMillis)
    } yield (start, end)
  }

  "Period.to" should {
    "create correct range of periods" in {
      forAll { (tuple: (Period, Instant)) =>
        tuple match {
          case (start, end) =>
            val actualDuration = (start to end).foldLeft(new Duration(0, 0)) {
              case (total, period) => 
                total.plus(period.size)
            }

            val expectedDuration = new Duration(start.start.getMillis, start.periodicity.period(end).end.getMillis)

            actualDuration.getMillis mustEqual (expectedDuration.getMillis)
        }
      } must pass
    }
  }

  "Period.until" should {
    "create correct range of periods" in {
      forAll { (tuple: (Period, Instant)) =>
        tuple match {
          case (start, end) =>
            val actualDuration = (start until end).foldLeft(new Duration(0, 0)) {
              case (total, period) => 
                total.plus(period.size)
            }

            val expectedDuration = new Duration(start.start.getMillis, start.periodicity.period(end).start.getMillis)

            actualDuration.getMillis mustEqual (expectedDuration.getMillis)
        }
      } must pass
    }
  }

}
