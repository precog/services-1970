package com.reportgrid.analytics

import Periodicity._

import blueeyes._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.persistence.mongo._
import blueeyes.util._

import org.joda.time.Instant

import com.reportgrid.util.MapUtil._
import scala.annotation.tailrec
import scala.collection.immutable.SortedMap
import scala.math.Ordered._
import scalaz._
import Scalaz._

case class TimeSeriesSpan[T: AbelianGroup] private (series: SortedMap[Period, T]) {
  def toTimeSeries(periodicity: Periodicity): TimeSeries[T] = error("todo")

  def flatten = series.values

  def total = series.values.asMA.sum
}

object TimeSeriesSpan {
  def apply[T: AbelianGroup](entries: (Period, T)*) = {
    val isValidSpan = entries.size <= 1 || {
      val sortedPeriods = entries.view.map(_._1).sorted
      sortedPeriods.zip(sortedPeriods.tail).foldLeft(true) { 
        case (false, _)     => false
        case (true, (a, b)) => a.end == b.start 
      }
    }

    if (isValidSpan) new TimeSeriesSpan(SortedMap(entries: _*))
    else error("The periods provided do not form a contiguous span")
  }
}

/** A time series stores an unlimited amount of time series data.
 */
case class TimeSeries[T] private (periodicity: Periodicity, series: Map[Instant, T])(implicit aggregator: AbelianGroup[T]) {
  /** Fill all gaps in the returned time series -- i.e. any period that does
   * not exist will be mapped to a count of 0. Note that this function may
   * behave strangely if the time series contains periods of a different
   * periodicity.
   */
  def fillGaps(start: Option[Instant], end: Option[Instant]): TimeSeries[T] = {
    import blueeyes.util._
    val instants = if (periodicity == Eternity) Stream(Instants.Zero)
                   else periodicity.period(start.getOrElse(series.keys.min)) datesTo end.getOrElse(series.keys.max)

    TimeSeries(
      periodicity, 
      instants.foldLeft(series) {
        (series, date) => series + (date -> (series.getOrElse(date, aggregator.zero)))
      }
    )
  }

  def aggregates = {
    @tailrec def superAggregates(periodicity: Periodicity, acc: List[TimeSeries[T]]): List[TimeSeries[T]] = {
      periodicity.nextOption match {
        case Some(p) => superAggregates(p, this.withPeriodicity(p).get :: acc)
        case None => acc
      }
    }

    superAggregates(periodicity, List(this))
  }

  def withPeriodicity(newPeriodicity: Periodicity): Option[TimeSeries[T]] = {
    if (newPeriodicity < periodicity) None
    else if (newPeriodicity == periodicity) Some(this)
    else Some(
      TimeSeries(
        newPeriodicity,
        series.foldLeft(Map.empty[Instant, T]) {
          case (series, entry) => addToMap(newPeriodicity, series, entry)
        }
      )
    )
  }

  def slice(start: Instant, end: Instant) = {
    val minTime = periodicity.floor(start)
    val maxTime = periodicity.ceil(end)
    TimeSeries(periodicity, series.filter(t => t._1 >= minTime && t._1 < maxTime))
  }

  /** Combines the data in this time series with the data in that time series.
   */
  def + (that: TimeSeries[T]): TimeSeries[T] = {
    TimeSeries(periodicity, series <+> that.series)
  }

  def + (entry: (Instant, T)): TimeSeries[T] = TimeSeries(periodicity, addToMap(periodicity, series, entry))

  def total: T = series.values.asMA.sum

  def toJValue(implicit d: Decomposer[T]): JValue = JObject(List(
    JField(
      periodicity.name, 
      JArray(series.toList.sortWith(_._1 < _._1).map { 
        case (time, count) => JArray(JInt(time.getMillis) :: count.serialize :: Nil)
      })
    )
  ))

  private def addToMap(p: Periodicity, m: Map[Instant, T], entry: (Instant, T)) = {
    val countTime = p.floor(entry._1)
    m + (countTime -> (m.getOrElse(countTime, aggregator.zero) |+| entry._2))
  }
}

object TimeSeries {
  def empty[T: AbelianGroup](periodicity: Periodicity): TimeSeries[T] = {
    new TimeSeries(periodicity, Map.empty[Instant, T])
  }

  def point[T: AbelianGroup](periodicity: Periodicity, time: Instant, value: T) = {
    new TimeSeries(periodicity, Map(periodicity.floor(time) -> value))
  }

  implicit def Semigroup[T: AbelianGroup] = Scalaz.semigroup[TimeSeries[T]](_ + _)
}

object TimeSeriesEncoding {
  private val DefaultGrouping = Map[Periodicity, Periodicity](
    Minute   -> Month,
    Hour     -> Year,
    Day      -> Year,
    Week     -> Eternity,
    Month    -> Eternity,
    Year     -> Eternity,
    Eternity -> Eternity
  )

  /** Returns an encoding for all periodicities from minute to eternity */
  val Default = new TimeSeriesEncoding(DefaultGrouping)

  /** Returns an aggregator for all periodicities from second to eternity */
  val All = new TimeSeriesEncoding(DefaultGrouping + (Second -> Day))
}

class TimeSeriesEncoding(val grouping: Map[Periodicity, Periodicity]) {
  def expand(start: Instant, end: Instant): Stream[Period] = {
    import Stream._

    def expandFiner(start: Instant, end: Instant, periodicity: Periodicity, 
                    expansion: (Instant, Instant, Periodicity) => Stream[Period]): Stream[Period] = {

      periodicity.finer.filter(grouping.contains).
      map(expansion(start, end, _)).
      getOrElse {
        val length = end.getMillis - start.getMillis
        val period = periodicity.period(start)

        if (length.toDouble / period.size.getMillis >= 0.5) period +: Empty else Empty
      }
    }

    def expandBoth(start: Instant, end: Instant, periodicity: Periodicity): Stream[Period] = {
      if (start.getMillis >= end.getMillis) Empty 
      else {
        val periods = periodicity.period(start) to end
        val head = periods.head
        val tail = periods.tail

        if (tail.isEmpty) expandFiner(start, end, periodicity, expandBoth _)
        else {
          expandFiner(start, head.end, periodicity, expandLeft _) ++ 
          tail.init ++ 
          expandFiner(tail.last.start, end, periodicity, expandRight _)
        }
      }
    }

    def expandLeft(start: Instant, end: Instant, periodicity: Periodicity): Stream[Period] = {
      if (start.getMillis >= end.getMillis) Empty 
      else {
        val periods = (periodicity.period(start) until end)
        expandFiner(start, periods.head.end, periodicity, expandLeft _) ++ periods.tail
      }
    }

    def expandRight(start: Instant, end: Instant, periodicity: Periodicity): Stream[Period] = {
      if (start.getMillis >= end.getMillis) Empty 
      else {
        val periods = (periodicity.period(start) to end)
        periods.init ++ expandFiner(periods.last.start, end, periodicity, expandRight _)
      }
    }

    expandBoth(start, end, Periodicity.Year)
  }

  def queriableExpansion(start: Instant, end: Instant): Stream[(Periodicity, Instant, Instant)] = {
    queriableExpansion(expand(start, end))
  }

  def queriableExpansion(expansion: Stream[Period]) = {
    def qe(expansion: Stream[Period], results: Stream[(Periodicity, Instant, Instant)]): Stream[(Periodicity, Instant, Instant)]  = {
      if (expansion.isEmpty) results
      else {
        val Period(periodicity, nstart, nend) = expansion.head
        results match {
          case Stream.Empty => qe(expansion.tail, (periodicity, nstart, nend) #:: results)
          case (`periodicity`, start, `nstart`) #:: rest => qe(expansion.tail, (periodicity, start, nend) #:: rest)
          case currentHead #:: rest => currentHead #:: qe(expansion.tail, (periodicity, nstart, nend) #:: rest)
        }
      }
    }

    qe(expansion, Stream.Empty)
  }
}
