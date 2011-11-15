package com.reportgrid.analytics

import blueeyes.util.Clock
import org.scalacheck._
import org.scalacheck.Gen._
import org.scalacheck.Prop._
import Periodicity._
import Arbitrary._

trait ArbitraryTime {
  val genTimeClock: Clock

  val genTime = for (i <- choose(0, 1000 * 60 * 60 * 48)) yield genTimeClock.now().plus(i).toInstant
  implicit val arbTime = Arbitrary(genTime)

  def genPeriodicity(pers: Periodicity*) = oneOf[Periodicity](pers)
  implicit val arbPeriodicity = Arbitrary(genPeriodicity(Periodicity.All: _*))

  def genPeriod(pers: Periodicity*) = for (time <- genTime; periodicity <- genPeriodicity(pers: _*)) yield Period(periodicity, time)
  implicit val arbPeriod = Arbitrary(genPeriod(Periodicity.All: _*))
}
