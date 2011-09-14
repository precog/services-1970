package com.reportgrid.analytics

import blueeyes.util._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.JodaSerializationImplicits._

import org.joda.time.Instant
import scalaz.Scalaz._

import SignatureGen._
import AnalyticsServiceSerialization._

sealed trait TagTerm {
  type StorageKeysType <: StorageKeys

  /**
   * The asymmetry between this signature and that of TagValue storage
   * key classes reflects the asymmetry between inserting at a single point
   * in the multidimensional space and querying over a subspace. For non-range
   * type queries, this map will return a point (i.e. a single key with a value 
   * that has a single member)
   */
  def storageKeys: Seq[(Sig, Seq[(Sig, JField)])]

  /**
   * Return the sequence of reference signatures that will be used to correlate to 
   * infinite value storage.
   */
  def infiniteValueKeys: Seq[Sig] 
}

case class IntervalTerm(encoding: TimeSeriesEncoding, resultGranularity: Periodicity, span: TimeSpan) extends TagTerm {
  type StorageKeysType = TimeRefKeys

  private def docStoragePeriods = span match {
    case TimeSpan.Eternity => Nil
    case TimeSpan.Finite(start, end) => 
      val docGranularity = encoding.grouping(resultGranularity)
      val pstart = docGranularity.period(start)
      val pend = docGranularity.period(end)

      if      (pstart == pend)           pstart :: Nil
      else if (pstart.end == pend.start) pstart :: pend :: Nil
      else                               sys.error("Query interval too large - too many results to return.")
  }

  private def dataKeyInstants(docStoragePeriod: Period) = span match {
    case TimeSpan.Eternity => Stream.empty[Instant]

    case TimeSpan.Finite(start, end) => 
      resultGranularity.period(docStoragePeriod.start max start).datesUntil(docStoragePeriod.end min end)
  }

  // see AggregationEngine.dataKeySigs._1
  override def storageKeys: Seq[(Sig, Stream[(Sig, JField)])] = {
    for (docPeriod <- docStoragePeriods) 
    yield ((resultGranularity, docPeriod).sig -> (dataKeyInstants(docPeriod).map(i => (i.sig, JField("timestamp", i.serialize)))))
  }

  // see AggregationEngine.dataKeySigs._2
  override def infiniteValueKeys: Stream[Sig] = span match {
    case TimeSpan.Eternity => Stream.empty[Sig]
    case TimeSpan.Finite(start, end) => 
      resultGranularity.period(start).datesUntil(end).map(instant => Sig(resultGranularity.sig, instant.sig))
  }
}

object IntervalTerm {
  def Eternity(encoding: TimeSeriesEncoding) = IntervalTerm(encoding, Periodicity.Eternity, TimeSpan.Eternity)
}

case class SpanTerm(encoding: TimeSeriesEncoding, span: TimeSpan) extends TagTerm {
  type StorageKeysType = TimeRefKeys

  override def storageKeys: Seq[(Sig, Stream[(Sig, JField)])] = span match {
    case TimeSpan.Eternity => IntervalTerm.Eternity(encoding).storageKeys
    case finite => encoding.queriableExpansion(span).flatMap {
      case (p, span) => IntervalTerm(encoding, p, span).storageKeys
    }
  }

  override def infiniteValueKeys: Stream[Sig] = span match {
    case TimeSpan.Eternity => Stream.empty[Sig]
    case finite => encoding.queriableExpansion(finite).flatMap {
      case (p, span) => IntervalTerm(encoding, p, span).infiniteValueKeys
    }
  }
}

case class HierarchyLocationTerm(tagName: String, location: Hierarchy.Location) extends TagTerm {
  type StorageKeysType = HierarchyKeys
  override def storageKeys: Seq[(Sig, List[(Sig, JField)])] = {
    location.path.parent.map(_.sig -> List((location.path.sig, JField(tagName, location.path.path.serialize)))).toSeq
  }

  override def infiniteValueKeys: List[Sig] = List(location.path.sig)
}

