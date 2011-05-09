package com.reportgrid.analytics

import org.joda.time.DateTime

case class TimeSeriesAggregator(periodicities: List[Periodicity]) {
  /** Aggregates the specified measure across all periodicities to produce a time series */
  def aggregate[T](time: DateTime, count: T)(implicit aggregator: Aggregator[T]): TimeSeries[T] = {
    val series = Map(
      periodicities.map { periodicity =>
        val id = Period(periodicity, time)

        id -> count
      }: _*
    )

    TimeSeries(series)
  }
}

object TimeSeriesAggregator {
  /** Returns an aggregator for all periodicities from minute to eternity */
  lazy val Default = TimeSeriesAggregator(Periodicity.Default)

  /** Returns an aggregator for all periodicities from second to eternity */
  lazy val All = TimeSeriesAggregator(Periodicity.All)

  /** Returns an aggregator for eternity */
  lazy val Eternity = TimeSeriesAggregator(Periodicity.Eternity :: Nil)
}
