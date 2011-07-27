package com.reportgrid.analytics

import blueeyes.util._
import org.joda.time.Instant
import scalaz.Scalaz._
import SignatureGen._

sealed trait DataTerm
case class ValueTerm(hasValue: HasValue) extends DataTerm
case class ChildTerm(hasChild: HasChild) extends DataTerm

sealed trait RangeTerm
case class TimeSpanTerm(span: TimeSpan) extends RangeTerm

sealed trait TagTerm {
  type StorageKeysType <: StorageKeys

  def tagName: String
  
  /**
   * The asymmetry between this signature and that of TagValue storage
   * key classes reflects the asymmetry between inserting at a single point
   * in the multidimensional space and querying over a subspace. For non-range
   * type queries, this map will return a point (i.e. a single key with a value 
   * that has a single member)
   */
  def storageKeys: Seq[(Sig, Seq[(Sig, StorageKeysType#DataKey)])]
}

case class IntervalTerm(encoding: TimeSeriesEncoding, resultGranularity: Periodicity, span: TimeSpan) extends TagTerm {
  type StorageKeysType = TimeRefKeys

  private def docStoragePeriods = span match {
    case TimeSpan.Eternity => Period.Eternity :: Nil
    case TimeSpan.Finite(start, end) => 
      val docGranularity = encoding.grouping(resultGranularity)
      val pstart = docGranularity.period(start)
      val pend = docGranularity.period(end)

      if      (pstart == pend)           pstart :: Nil
      else if (pstart.end == pend.start) pstart :: pend :: Nil
      else                               sys.error("Query interval too large - too many results to return.")
  }

  private def dataKeyInstants(docStoragePeriod: Period) = span match {
    case TimeSpan.Eternity => 
      Stream(Instants.Zero)

    case TimeSpan.Finite(start, end) => 
      resultGranularity.period(docStoragePeriod.start max start).datesTo(docStoragePeriod.end min end)
  }

  val tagName = "timestamp"

  // see AggregationEngine.dataKeySigs._1
  def storageKeys: Seq[(Sig, Stream[(Sig, Instant)])] = {
    for (docPeriod <- docStoragePeriods) yield (docPeriod.sig -> (dataKeyInstants(docPeriod).map(i => i.sig -> i)))
  }

  // see AggregationEngine.dataKeySigs._2
  def infiniteValueKeys: Stream[Sig] = span match {
    case TimeSpan.Eternity => error("todo")
    case TimeSpan.Finite(start, end) => 
      resultGranularity.period(start).datesTo(end).map(i => (resultGranularity, i).sig)
  }
}

object IntervalTerm {
  def Eternity(encoding: TimeSeriesEncoding) = IntervalTerm(encoding, Periodicity.Eternity, TimeSpan.Eternity)
}

case class HierarchyLocationTerm(tagName: String, location: Hierarchy.Location) extends TagTerm {
  type StorageKeysType = HierarchyKeys
  def storageKeys: Seq[(Sig, List[(Sig, Path)])] = 
    location.path.parent.map(_.sig -> List(location.path.sig -> location.path)).toSeq
}

