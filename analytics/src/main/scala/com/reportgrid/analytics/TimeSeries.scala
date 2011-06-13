package com.reportgrid.analytics

import Periodicity._

import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.persistence.mongo._

import org.joda.time.{DateTime, DateTimeZone}

import com.reportgrid.util.MapUtil._
import scalaz._
import Scalaz._

/** A time series stores an unlimited amount of time series data.
 * TODO: Maybe this map should be specialized to a periodicity of a single type?
 */
case class TimeSeries[T](series: Map[Period, T])(implicit aggregator: AbelianGroup[T]) {
  def flatten: List[T] = series.values.toList

  /** Groups the time series by period.
   */
  def groupByPeriod: Map[Period, TimeSeries[T]] = series.transform { 
    (period, count) => TimeSeries[T](Map[Period, T](period -> count))
  }

  /** Groups the time series by periodicity.
   */
  def groupByPeriodicity: Map[Periodicity, TimeSeries[T]] = {
    series.groupBy(_._1.periodicity).mapValues(TimeSeries(_))
  }

  /** Returns a "friendly" JSON rendering of the time series, sorted first by
   * periodicity, and then by period start.
   * {{{
   *  {"minute":[[1301279340000,10],[1301279520000,10]]}
   * }}}
   */
  def toJValue(implicit d: Decomposer[T]): JValue = JObject(groupByPeriodicity.toList.sortBy(_._1).map { 
    case (periodicity, series) => JField(
      periodicity.name, 
      JArray(series.series.toList.sortWith(_._1 < _._1).map { 
        case (period, count) => JArray(JInt(period.start.getMillis) :: count.serialize :: Nil)
      })
    )
  })

  /** Fill all gaps in the returned time series -- i.e. any period that does
   * not exist will be mapped to a count of 0. Note that this function may
   * behave strangely if the time series contains periods of a different
   * periodicity.
   */
  def fillGaps: TimeSeries[T] = {
    if (series.size == 0) this
    else {
      implicit val ordering = Ordering.ordered[Period]

      val periods = series.keys

      val minPeriod = periods.min
      val maxPeriod = periods.max

      val filledSeries = (minPeriod to maxPeriod.start).foldLeft(series) { (series, period) =>
        val count = series.get(period)

        if (!count.isEmpty) series
        else series + (period -> aggregator.zero)
      }

      TimeSeries(filledSeries)
    }
  }

  /** Combines the data in this time series with the data in that time series.
   */
  def + (that: TimeSeries[T]): TimeSeries[T] = TimeSeries(this.series <+> that.series)

  def + (entry: (Period, T)): TimeSeries[T]  = TimeSeries(this.series <+> Map(entry))

  /** Returns total.
   */
  def total(p: Periodicity): T = groupByPeriodicity.getOrElse(p, TimeSeries.empty[T]).series.values.asMA.sum

  def unary_- = TimeSeries(series.transform((k, v) => aggregator.inverse(v)))
}

object TimeSeries {
  def empty[T: AbelianGroup]: TimeSeries[T] = apply[T](Map.empty[Period, T])

  def apply[T: AbelianGroup](time: DateTime, count: T) = TimeSeriesEncoding.default[T].encode(time, count)

  def all[T: AbelianGroup](time: DateTime, count: T) = TimeSeriesEncoding.all[T].encode(time, count)

  def eternity[T: AbelianGroup](time: DateTime, count: T) = TimeSeriesEncoding.eternity[T].encode(time, count)
}

object TimeSeriesEncoding {
  val DefaultGrouping = Map[Periodicity, Periodicity](
    Minute   -> Month,
    Hour     -> Year,
    Day      -> Year,
    Week     -> Eternity,
    Month    -> Eternity,
    Year     -> Eternity,
    Eternity -> Eternity
  )

  /** Returns an encoding for all periodicities from minute to eternity */
  def default[T: AbelianGroup] = new TimeSeriesEncoding[T](DefaultGrouping)

  /** Returns an aggregator for all periodicities from second to eternity */
  def all[T: AbelianGroup] = new TimeSeriesEncoding[T](DefaultGrouping + (Second -> Day))

  def eternity[T: AbelianGroup] = new TimeSeriesEncoding[T](Map(Eternity -> Eternity))
}

class TimeSeriesEncoding[T: AbelianGroup](val grouping: Map[Periodicity, Periodicity]) {
  def encode(time: DateTime, count: T): TimeSeries[T] = {
    TimeSeries(grouping.keys.map(Period(_, time) -> count).toMap)
  }

  def expand(start: DateTime, end: DateTime): Stream[Period] = {
    def expand0(start: DateTime, end: DateTime, periodicity: Periodicity): Stream[Period] = {
      import Stream._

      def expandFiner(start: DateTime, end: DateTime, periodicity: Periodicity): Stream[Period] = {
        periodicity.previousOption.filter(grouping.contains).
        map(expand0(start, end, _)).
        getOrElse {
          val length = end.getMillis - start.getMillis
          val period = periodicity.period(start)

          if (length.toDouble / period.size.getMillis >= 0.5) period +: Empty else Empty
        }
      }

      if (start.getMillis >= end.getMillis) Empty 
      else {
        val periods = periodicity.period(start) to end
        val head = periods.head
        val tail = periods.tail

        if (periods.tail.isEmpty) expandFiner(start, end, periodicity)
        else {
          expandFiner(start, head.end, periodicity) ++ 
          tail.init ++ 
          expandFiner(tail.last.start, end, periodicity)
        }
      }
    }

    expand0(start, end, Periodicity.Year)
  }
}
