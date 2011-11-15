package com.reportgrid.analytics

import org.joda.time.Instant

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.scalacheck._
import org.scalacheck.Gen._
import org.scalacheck.Prop._

import Periodicity._
import Arbitrary._

class PeriodicitySpec extends Specification with ArbitraryTime with ScalaCheck {
  val genTimeClock = blueeyes.util.Clock.System

  case class Count(i: Int)
  "Periodicity.period" should {
    "have the same semantics as period.withPeriodicity" in {
      check {
        (periodicity: Periodicity, period: Period) => period.withPeriodicity(periodicity) must_== periodicity.period(period.start)
      }
    }
  }

  "Periodicity.floor" should {
    "return the same value for any number of calls" in {
      def floorN(periodicity: Periodicity, time: Instant, n: Int): Instant = {
        if (n > 1) floorN(periodicity, periodicity.floor(time), n - 1)
        else periodicity.floor(time)
      }

      implicit val arbCount = Arbitrary(choose(1, 10).map(Count))

      check {
        (periodicity: Periodicity, time: Instant, count: Count) => (floorN(periodicity, time, count.i) must_== periodicity.floor(time)) 
      }
    }

    "always be less than or equal to the specified time" in {
      check { (time: Instant, periodicity: Periodicity) =>
        periodicity.floor(time).getMillis must beLessThanOrEqualTo(time.getMillis)
      } 
    }
  }
}


// vim: set ts=4 sw=4 et:
