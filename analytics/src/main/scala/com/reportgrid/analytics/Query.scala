package com.reportgrid.analytics

import blueeyes.util._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.JodaSerializationImplicits._
import blueeyes.json.xschema.DefaultSerialization._

import org.joda.time.Instant
import scalaz.Scalaz._

import SignatureGen._
import AnalyticsServiceSerialization._

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
  def storageKeys: Seq[(Sig, Seq[(Sig, JField)])]
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
  def storageKeys: Seq[(Sig, Stream[(Sig, JField)])] = {
    for (docPeriod <- docStoragePeriods) 
    yield ((resultGranularity, docPeriod).sig -> (dataKeyInstants(docPeriod).map(i => (i.sig, JField(tagName, i.serialize)))))
  }

  // see AggregationEngine.dataKeySigs._2
  def infiniteValueKeys: Stream[Sig] = span match {
    case TimeSpan.Eternity => error("todo")
    case TimeSpan.Finite(start, end) => 
      resultGranularity.period(start).datesTo(end).map(instant => Sig(resultGranularity.sig, instant.sig))
  }
}

object IntervalTerm {
  def Eternity(encoding: TimeSeriesEncoding) = IntervalTerm(encoding, Periodicity.Eternity, TimeSpan.Eternity)
}

case class HierarchyLocationTerm(tagName: String, location: Hierarchy.Location) extends TagTerm {
  type StorageKeysType = HierarchyKeys
  def storageKeys: Seq[(Sig, List[(Sig, JField)])] = {
    location.path.parent.map(_.sig -> List((location.path.sig, JField(tagName, location.path.path.serialize)))).toSeq
  }
}

