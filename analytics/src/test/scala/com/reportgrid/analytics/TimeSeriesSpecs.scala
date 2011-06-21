package com.reportgrid.analytics

import org.joda.time.{DateTime, Duration}
import org.specs.{Specification, ScalaCheck}
import org.specs.specification.PendingUntilFixed
import org.scalacheck._
import org.scalacheck.Gen._
import org.scalacheck.Prop._
import Periodicity._
import Arbitrary._

import scalaz.Scalaz._

class TimeSeriesSpec extends Specification with ArbitraryTime with ScalaCheck {
  "TimeSeries.fillGaps" should {
    "create a time series where there are no gaps" in {
      forAllNoShrink(genTime, genTime, genPeriodicity(Periodicity.All: _*)) { (time1: DateTime, time2: DateTime, periodicity: Periodicity) => 
        (periodicity != Second) ==> {
          val (start, end) = (if (time1.getMillis < time2.getMillis) (time1, time2) else (time2, time1)).mapElements(Minute.floor, Minute.floor)
          
          val allDates = periodicity.period(start).datesTo(end).toSet
          val testSeries = allDates.filter(_ => scala.util.Random.nextBoolean).foldLeft(TimeSeries.empty[Int](periodicity) + ((start, 0)) + ((end, 0))) {
            (series, date) => series + ((date, scala.util.Random.nextInt))
          }

          testSeries.fillGaps.series.keySet == allDates
        }
      } must pass
    }
  }
}

class TimeSeriesEncodingSpec extends Specification with ArbitraryTime with ScalaCheck {
  "TimeSeriesEncoding.expand" should {
    val encoding = TimeSeriesEncoding.Default

    "correctly expand over a known period" in {
      val start = Minute.floor(new DateTime("2011-06-20T17:36:33.474-06:00"))
      val end =   Minute.floor(new DateTime("2011-06-21T09:23:02.005-06:00"))

      val expectedDuration = new Duration(start, end)
       
      val actualDuration = encoding.expand(start, end).foldLeft(new Duration(0, 0)) {
        case (totalSize, period) => totalSize.plus(period.size)
      }

      actualDuration must_== expectedDuration
    }

    "create periods whose total size is close to the duration between start and end" in {
      forAll { (time1: DateTime, time2: DateTime) =>
        val (start, end) = (if (time1.getMillis < time2.getMillis) (time1, time2) else (time2, time1)).mapElements(Minute.floor, Minute.floor)

        val expectedDuration = new Duration(start, end)

        val actualDuration = encoding.expand(start, end).foldLeft(new Duration(0, 0)) {
          case (totalSize, period) => totalSize.plus(period.size)
        }

        actualDuration.getMillis mustEqual (expectedDuration.getMillis)
      } must pass
    }

    "create a series where smaller periods are not sandwiched by larger periods" in {
      forAll { (time1: DateTime, time2: DateTime) =>
        val (start, end) = (if (time1.getMillis < time2.getMillis) (time1, time2) else (time2, time1)).mapElements(Minute.floor, Minute.floor)

        val expansion = encoding.expand(start, end)
        expansion.foldLeft((true, Option.empty[Periodicity])) {
          case ((true, None), period) => (true, Some(period.periodicity))
          case ((true, Some(periodicity)), period) =>
            //if it is currently increasing, periodicity granularity may either increase or decrease
            (period.periodicity >= periodicity, Some(period.periodicity))

          case ((false, Some(periodicity)), period) =>
            //if it is currently decreasing, periodicity granularity must decrease
            if (period.periodicity > periodicity) error("Time series has the wrong shape.")
            (false, Some(period.periodicity))
        }

        true
      } must pass
    }
  }

  "TimeSeriesEncoding.queriableExpansions" should {
    val encoding = TimeSeriesEncoding.Default

    "create subsequences whose total size is close to the duration between start and end" in {
      forAll { (time1: DateTime, time2: DateTime) =>
        val (start, end) = (if (time1.getMillis < time2.getMillis) (time1, time2) else (time2, time1)).mapElements(Minute.floor, Minute.floor)

        val expectedDuration = new Duration(start, end)

        val actualDuration = encoding.queriableExpansion(start, end).foldLeft(new Duration(0, 0)) {
          case (totalSize, (_, start, end)) => totalSize.plus(new Duration(start, end))
        }

        actualDuration.getMillis mustEqual (expectedDuration.getMillis)
      } must pass
    }

    "have an expansion that has at most two elements of each periodicity" in {
      forAll { (time1: DateTime, time2: DateTime) =>
        val (start, end) = (if (time1.getMillis < time2.getMillis) (time1, time2) else (time2, time1)).mapElements(Minute.floor, Minute.floor)

        val expansion = encoding.expand(start, end)
        encoding.queriableExpansion(expansion).groupBy(_._1).forall {
          case (k, v) => v.size <= 2
        }
      } must pass
    }

  }
}
