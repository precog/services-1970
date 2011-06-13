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

class TimeSeriesEncodingSpec extends Specification with ArbitraryTime with ScalaCheck {
  "TimeSeriesEncoding.expand" should {
    val encoding = TimeSeriesEncoding.default[Long]

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
  }

}
