package com.reportgrid.analytics

import blueeyes.util.Clock
import org.scalacheck._
import org.scalacheck.Gen._
import org.scalacheck.Prop._
import Periodicity._
import Arbitrary._
import scalaz.Scalaz._
import scalaz.Monad

trait ArbitraryTime {
  val genTimeClock: Clock

  implicit object GenMonad extends Monad[Gen] {
    def pure[A](a: => A) = value(a)
    def bind[A, B](m: Gen[A], f: A => Gen[B]) = m.flatMap(f)
  }

  val genTimePeriodicities: Map[Periodicity, Int] = Map(
    Second -> 60,
    Minute -> 60,
    Hour -> 24,
    Day -> 31,
    Week -> 52,
    Month -> 12,
    Year -> 3
  )

  val genTime = {
    for {
      periodicities <- pick(3, genTimePeriodicities)
      offsets <- periodicities map { case (periodicity, max) => choose(0, max).map(periodicity.jodaPeriod(_).get) } sequence
    } yield {
      offsets.foldLeft(genTimeClock.now()) { (date, offset) => date.plus(offset) } toInstant
    }
  }

  implicit val arbTime = Arbitrary(genTime)

  def genPeriodicity(pers: Periodicity*) = oneOf[Periodicity](pers)
  implicit val arbPeriodicity = Arbitrary(genPeriodicity(Periodicity.All: _*))

  def genPeriod(pers: Periodicity*) = for (time <- genTime; periodicity <- genPeriodicity(pers: _*)) yield Period(periodicity, time)
  implicit val arbPeriod = Arbitrary(genPeriod(Periodicity.All: _*))
}
