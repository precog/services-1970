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
import org.joda.time.Duration
import org.joda.time.ReadableDuration

import com.reportgrid.ct._
import com.reportgrid.util.MapUtil._
import scala.annotation.tailrec
import scala.collection.immutable.SortedMap
import scala.math.Ordered._
import scala.math.Ordering
import scalaz.Scalaz._
import scalaz.Semigroup

case class TimeSeriesSpan[T: AbelianGroup] private (series: SortedMap[Period, T]) {
  def toTimeSeries(periodicity: Periodicity): TimeSeries[T] = sys.error("todo")

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
    else sys.error("The periods provided do not form a contiguous span")
  }
}

/** A time series stores an unlimited amount of time series data.
 */
case class TimeSeries[T] private (periodicity: Periodicity, series: Map[Instant, T])(implicit aggregator: AbelianGroup[T]) {
  def deltaSet: Option[DeltaSet[Instant, ReadableDuration, T]] = {
    implicit val periodicitySpace: DeltaSpace[Instant, ReadableDuration] = new PeriodicitySpace(periodicity)
    implicit val instantSAct: SAct[Instant, ReadableDuration] = InstantSAct
    DeltaSet(series)
  }

  def groupBy(grouping: Periodicity): Option[DeltaSet[Int, Int, T]] = {
    import SAct._
    (!series.isEmpty).option {
      implicit val deltaSpace: DeltaSpace[Int, Int] = CountingSpace
      new DeltaSet(0,
        series.foldLeft(Map.empty[Int, T]) {
          case (m, (instant, value)) =>
            periodicity.indexOf(instant, grouping) map { index => 
              m + (index -> m.get(index).map(_ |+| value).getOrElse(value))
            } getOrElse {
              sys.error("Cannot group time series of periodicity " + periodicity + " by " + grouping)
            }
        } 
      )
    }
  }

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

  def map[U: AbelianGroup](f: T => U): TimeSeries[U] = TimeSeries(periodicity, series.mapValues(f))

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
          (series, entry) => addToMap(newPeriodicity, series, entry)
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

  implicit def Semigroup[T: AbelianGroup] = semigroup[TimeSeries[T]](_ + _)
}

/**
 * A generalization of time series and other types that can be encoded using
 * delta compression. 
 */
class DeltaSet[A, D, V](val zero: A, val data: Map[D, V])(implicit sact: SAct[A, D], d: DeltaSpace[A, D], ord: Ordering[A], group: AbelianGroup[V]) {
  lazy val series: SortedMap[A, V] = data.foldLeft(SortedMap.empty[A, V]) {
    case (m, (d, v)) => 
      val key = sact.append(zero, d)
      m + (key -> (m.getOrElse(key, group.zero) |+| v))
  }

  def fillGaps(start: Option[A], end: Option[A]): DeltaSet[A, D, V] = {
    def streamFrom(origin: A, to: A): Stream[D] = {
      import Stream._
      def _streamFrom(from: A): Stream[D] = {
        if (from >= to) empty[D]
        else cons(d.difference(origin, from), _streamFrom(sact.append(from, d(from))))
      }

      _streamFrom(origin)
    }

    val filled = streamFrom(start.getOrElse(zero), end.getOrElse(series.keySet.max)).foldLeft(data) {
      (data, delta) => data + (delta -> (data.getOrElse(delta, group.zero)))
    }

    new DeltaSet(start.map(ord.min(_, zero)).getOrElse(zero), filled)
  }

  def + (that: DeltaSet[A, D, V]) = new DeltaSet(zero, data <+> that.data)

  def + (entry: (A, V)): DeltaSet[A, D, V] = new DeltaSet(zero, data + (d.difference(_: A, zero)).first.apply(entry))

  def map[X: AbelianGroup](f: V => X): DeltaSet[A, D, X] = {
    new DeltaSet(zero, data.mapValues(f))
  }

  def total: V = data.values.asMA.sum
}

object DeltaSet {
  def apply[A, D, V](series: Map[A, V])(implicit sact: SAct[A, D], d: DeltaSpace[A, D], ord: Ordering[A], group: AbelianGroup[V]): Option[DeltaSet[A, D, V]] = {
    (!series.isEmpty).option {
      val zero = series.keySet.min
      new DeltaSet(zero, series.map((d.difference(zero, _: A)).first))
    }
  }
}

/** 
 * A metric space for deltas. Type A refers to a measurement type, such as an instant or a
 * geolocation; type B refers to the type that represents the difference between two
 * values of type A. For instants, this is a duration; for geolocation, this is a vector
 * on an oblate spheroid.
 */
trait DeltaSpace[-A, +B] {
  /** Returns the incremental delta amount given the base value a. */
  def apply(a: A): B
  def difference(a1: A, a2: A): B
}

class PeriodicitySpace(periodicity: Periodicity) extends DeltaSpace[Instant, ReadableDuration] {
  def apply(i: Instant) = new Duration(i, periodicity.increment(i))  
  def difference(i1: Instant, i2: Instant) = new Duration(i1, i2)
}

object InstantSAct extends SAct[Instant, ReadableDuration] {
  def append(i: Instant, d: => ReadableDuration) = i.plus(d)
}

object CountingSpace extends DeltaSpace[Int, Int] {
  def apply(i: Int): Int = 1
  def difference(i1: Int, i2: Int): Int = i2 - i1
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
