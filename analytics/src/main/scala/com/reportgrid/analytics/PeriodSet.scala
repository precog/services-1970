package com.reportgrid.analytics

import org.joda.time.Instant
import scalaz.Scalaz._
import blueeyes.json.JsonAST._
import blueeyes.json.Printer._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._

sealed trait PeriodSet {
  def mapBatchPeriods[T](batchPeriodicity: Periodicity)(f: Period => T): Iterable[T] 

  def deserializeTimeSeries[T : Extractor : AbelianGroup](jobj: JObject): TimeSeries[T]

  protected def mapBatchPeriods[T](min: Option[Instant], max: Option[Instant], granularity: Periodicity, batchPeriodicity: Periodicity)(f: Period => T): Iterable[T] = {
    (min.map(batchPeriodicity.period), max.map(batchPeriodicity.period)) match {
      case (Some(start), Some(end)) => 
        if      (start == end)           f(start) :: Nil
        else if (start.end == end.start) f(start) :: f(end) :: Nil
        else                             sys.error("Query interval too large - too many results to return.")

      case (ostart, oend) => 
        ostart.
        orElse(oend).
        orElse((granularity == Periodicity.Eternity).option(Period.Eternity)).
        map(f)
    }
  }
}

case class Interval(start: Option[Instant], end: Option[Instant], granularity: Periodicity) extends PeriodSet {
  def mapBatchPeriods[T](batchPeriodicity: Periodicity)(f: Period => T): Iterable[T] = {
    mapBatchPeriods(start, end, granularity, batchPeriodicity)(f)
  }

  def deserializeTimeSeries[T : Extractor : AbelianGroup](obj: JObject): TimeSeries[T] = {
    val startFloor = start.map(granularity.floor)
    obj.fields.foldLeft(TimeSeries.empty[T](granularity)) {
      case (series, JField(time, count)) => 
        val ltime = time.toLong 
        if (startFloor.forall(_.getMillis <= ltime) && end.forall(_.getMillis > ltime)) 
           series + ((new Instant(ltime), count.deserialize[T]))
        else series
    } 
  }
}

// vim: set ts=4 sw=4 et:
