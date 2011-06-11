package com.reportgrid.analytics

import org.joda.time.DateTime
import org.specs.{Specification, ScalaCheck}
import org.specs.specification.PendingUntilFixed
import org.scalacheck._
import org.scalacheck.Gen._
import org.scalacheck.Prop._
import Periodicity._
import Arbitrary._

trait ArbitraryTime {
  val Now = new DateTime()

  val genTime = for (i <- choose(0, (1000 * 60 * 60 * 24))) yield Now.plusMillis(i)
  implicit val arbTime = Arbitrary(genTime)

  val genPeriodicity = oneOf[Periodicity](Periodicity.All)
  implicit val arbPeriodicity = Arbitrary(genPeriodicity)

  val genPeriod = for (time <- genTime; periodicity <- genPeriodicity) yield Period(periodicity, time)
  implicit val arbPeriod = Arbitrary(genPeriod)
}

class PeriodicitySpec extends Specification with ArbitraryTime with ScalaCheck {
  case class Count(i: Int)
  "Periodicity" should {
    "Periodicity.period must have the same semantics as period.withPeriodicity" in {
      val prop = forAll {
        (periodicity: Periodicity, period: Period) => period.withPeriodicity(periodicity) must_== periodicity.period(period.start)
      }

      prop must pass
    }

    "Periodicity.floor must return the same value for any number of calls" in {
      def floorN(periodicity: Periodicity, time: DateTime, n: Int): DateTime = {
        if (n > 1) floorN(periodicity, periodicity.floor(time), n - 1)
        else periodicity.floor(time)
      }

      implicit val arbCount = Arbitrary(choose(1, 10).map(Count))

      val prop = forAll {
        (periodicity: Periodicity, time: DateTime, count: Count) => (floorN(periodicity, time, count.i) must_== periodicity.floor(time)) 
      }

      prop must pass
    }
  }

}


// vim: set ts=4 sw=4 et:
