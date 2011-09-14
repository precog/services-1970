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

/**
 * A generalization of time series and other types that can be encoded using
 * delta compression. 
 */
class DeltaSet[A, D, V](val zero: A, val data: SortedMap[D, V])
                       (implicit sact: SAct[A, D], val deltaSpace: DeltaSpace[A, D], aord: Ordering[A], dord: Ordering[D], group: AbelianGroup[V]) {
  lazy val series: SortedMap[A, V] = data.foldLeft(SortedMap.empty[A, V]) {
    case (m, (deltaSpace, v)) => 
      val key = sact.append(zero, deltaSpace)
      m + (key -> (m.getOrElse(key, group.zero) |+| v))
  }

  def fillGaps(start: Option[A], end: Option[A]): DeltaSet[A, D, V] = {
    def streamFrom(origin: A, to: A): Stream[D] = {
      import Stream._
      def _streamFrom(from: A): Stream[D] = {
        if (from >= to) empty[D]
        else cons(deltaSpace.difference(origin, from), _streamFrom(sact.append(from, deltaSpace(from))))
      }

      _streamFrom(origin)
    }

    val filled = streamFrom(start.getOrElse(zero), end.getOrElse(series.keySet.max)).foldLeft(data) {
      (data, delta) => data + (delta -> (data.getOrElse(delta, group.zero)))
    }

    new DeltaSet(start.map(aord.min(_, zero)).getOrElse(zero), filled)
  }

  def + (that: DeltaSet[A, D, V]) = new DeltaSet(zero, data |+| that.data)

  def + (entry: (A, V)): DeltaSet[A, D, V] = new DeltaSet(zero, data + (deltaSpace.difference(_: A, zero)).first.apply(entry))

  def map[X: AbelianGroup](f: V => X): DeltaSet[A, D, X] = {
    new DeltaSet(zero, data.map(f.second[D]))
  }

  def total: V = data.values.asMA.sum
}

object DeltaSet {
  def apply[A, D, V](series: SortedMap[A, V])
                    (implicit sact: SAct[A, D], d: DeltaSpace[A, D], aord: Ordering[A], dord: Ordering[D], group: AbelianGroup[V]): Option[DeltaSet[A, D, V]] = {
    (!series.isEmpty).option {
      val zero = series.keySet.min
      new DeltaSet(zero, series.map((d.difference(zero, _: A)).first[V]))
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

  def expand(start: Instant, end: Instant, periodicity: Periodicity = Periodicity.Year): Stream[Period] = {
    if (start.getMillis >= end.getMillis) Empty 
    else {
      val periods = periodicity.period(start) to end
      val head = periods.head
      val tail = periods.tail

      if (tail.isEmpty) expandFiner(start, end, periodicity, expand _)
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

  /**
   * The queriable exapansion of a time span is the set of spans of maximal granularity
   * that exactly cover the given span, given the grouping specified.
   * For example, consider a span from 12:30:30T2011-05-10 to 14:00:00T2011-08-04.
   * This span, given the default grouping, will have the shape: 
   * Stream((minutes, t1, t2), (hours, ...), (days, ...), (months, ...), (days, ...), (hours, ...))
   * The objective of this expansion is to make it possible to query the minimal set of documents.
   */
  def queriableExpansion(span: TimeSpan): Stream[(Periodicity, TimeSpan)] = {
    queriableExpansion(expand(span.start, span.end))
  }

  def queriableExpansion(expansion: Stream[Period]) = {
    def qe(expansion: Stream[Period], results: Stream[(Periodicity, TimeSpan)]): Stream[(Periodicity, TimeSpan)]  = {
      if (expansion.isEmpty) results
      else {
        val Period(periodicity, nstart, nend) = expansion.head
        results match {
          case Stream.Empty => 
            qe(expansion.tail, (periodicity, TimeSpan(nstart, nend)) #:: results)

          case (`periodicity`, TimeSpan(start, `nstart`)) #:: rest => 
            qe(expansion.tail, (periodicity, TimeSpan(start, nend)) #:: rest)

          case currentHead #:: rest => 
            currentHead #:: qe(expansion.tail, (periodicity, TimeSpan(nstart, nend)) #:: rest)
        }
      }
    }

    qe(expansion, Stream.Empty)
  }
}
