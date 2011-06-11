package com.reportgrid.analytics

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

      val filledSeries = (minPeriod to maxPeriod).foldLeft(series) { (series, period) =>
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

  def apply[T: AbelianGroup](time: DateTime, count: T) = Builder.Default(time, count)

  def all[T: AbelianGroup](time: DateTime, count: T) = Builder.All(time, count)

  def eternity[T: AbelianGroup](time: DateTime, count: T) = Builder.Eternity(time, count)

  case class Builder(periodicities: List[Periodicity]) {
    /** Aggregates the specified measure across all periodicities to produce a time series */
    def apply[T: AbelianGroup](time: DateTime, count: T): TimeSeries[T] = {
      TimeSeries(periodicities.map(Period(_, time) -> count).toMap)
    }
  }

  object Builder {
    /** Returns an aggregator for all periodicities from minute to eternity */
    lazy val Default = Builder(Periodicity.Default)

    /** Returns an aggregator for all periodicities from second to eternity */
    lazy val All = Builder(Periodicity.All)

    /** Returns an aggregator for eternity */
    lazy val Eternity = Builder(Periodicity.Eternity :: Nil)
  }
}
