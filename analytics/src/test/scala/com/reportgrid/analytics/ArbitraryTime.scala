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

  def genPeriodicity(pers: Periodicity*) = oneOf[Periodicity](pers)
  implicit val arbPeriodicity = Arbitrary(genPeriodicity(Periodicity.All: _*))

  def genPeriod(pers: Periodicity*) = for (time <- genTime; periodicity <- genPeriodicity(pers: _*)) yield Period(periodicity, time)
  implicit val arbPeriod = Arbitrary(genPeriod(Periodicity.All: _*))
}