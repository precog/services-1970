package com.reportgrid.analytics
package persistence

import blueeyes.json.JsonAST._
import blueeyes.json.{JPath, JPathNode, JPathField, JPathIndex, JPathImplicits}
import blueeyes.json.JsonParser.{parse}
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.JodaSerializationImplicits._

import blueeyes.persistence.mongo._

import blueeyes.util.SpecialCharTranscoder

import com.reportgrid.analytics._

import org.joda.time.{DateTime, Instant}
import scalaz.Scalaz._

/** Support for persitence via MongoDB.
 */
object MongoSupport extends AnalyticsSerialization {
  val MongoEscaper = SpecialCharTranscoder.fromMap('/',
    Map(
      '.' -> '*',
      '$' -> 'S'
    )
  )

  implicit val PeriodDecomposer = new Decomposer[Period] {
    def decompose(period: Period): JValue = JObject(
      JField("periodicity", period.periodicity.serialize) ::
      JField("start",       period.start.serialize) ::
      JField("end",         period.end.serialize) ::
      Nil
    )
  }

  implicit val PeriodExtractor = new Extractor[Period] {
    def extract(value: JValue): Period = Period(
      (value \ "periodicity").deserialize[Periodicity],
      (value \ "start").deserialize[Instant]
    )
  }

  implicit def TimeSeriesDecomposer[T](implicit decomposer: Decomposer[T]) = new Decomposer[TimeSeries[T]] {
    def decompose(value: TimeSeries[T]): JValue = JObject(List(
      JField("periodicity", JString(value.periodicity.name)),
      JField("data", JObject(value.series.map { case (time, count) => JField(time.getMillis.toString, count.serialize) }.toList))
    ))
  }

  implicit def TimeSeriesExtractor[T](implicit aggregator: AbelianGroup[T], extractor: Extractor[T]) = new Extractor[TimeSeries[T]] {
    def extract(value: JValue): TimeSeries[T] = {
      val periodicity = (value \ "periodicity").deserialize[Periodicity]
      value \ "data" match {
        case JObject(fields) =>
          fields.foldLeft(TimeSeries.empty[T](periodicity)) { 
            case (series, JField(timeString, value)) =>
              series + (new Instant(timeString.toLong) -> value.deserialize[T])
          }

        case _ => sys.error("Expected object but found: " + value)
      }
    }
  }

  implicit val ValueStatsExtractor: Extractor[ValueStats] = new Extractor[ValueStats] {
    def extract(value: JValue): ValueStats = ValueStats(
      (value \ "count").deserialize[Long],
      (value \? "sum").map(_.deserialize[Double]),
      (value \? "sumsq").map(_.deserialize[Double])
    )
  }

  implicit val VariableDecomposer = new Decomposer[Variable] {
    def decompose(v: Variable): JValue = v.name match {
      case JPath.Identity => "id"

      case jpath => jpath.serialize
    }
  }

  implicit val VariableExtractor = new Extractor[Variable] {
    def extract(v: JValue): Variable = v match {
      case JString("id") => Variable(JPath.Identity)

      case _ => Variable(v.deserialize[JPath])
    }
  }

  implicit def ObservationDecomposer[S <: Predicate](implicit pd: Decomposer[S]): Decomposer[Observation[S]] = new Decomposer[Observation[S]] {
    def decompose(v: Observation[S]): JValue = {
      JObject(
        v.toList.map { tuple =>
          val (variable, predicate) = tuple

          val name = if (variable.name == JPath.Identity) "id"
                     else MongoEscaper.encode(variable.name.toString)

          JField(name, predicate.serialize)
        }
      )
    }
  }

  implicit def ObservationExtractor[S <: Predicate](implicit pe: Extractor[S]): Extractor[Observation[S]] = new Extractor[Observation[S]] {
    def extract(v: JValue): Observation[S] = {
      val fields = (v --> classOf[JObject]).fields

      Set(fields.map { field =>
        val variable  = if (field.name == "id") Variable(JPath.Identity) else Variable(JPath(MongoEscaper.decode(field.name)))
        val predicate = field.value.deserialize[S]

        (variable, predicate)
      }: _*)
    }
  }

  implicit def ObservationCountDecomposer[T, S <: Predicate](implicit td: Decomposer[T], pd: Decomposer[S]) = new Decomposer[(Observation[S], T)] {
    def decompose(v: (Observation[S], T)): JValue = {
      val (observation, count) = v

      JObject(
        JField("where", observation.serialize(ObservationDecomposer)) ::
        JField("count", count.serialize) ::
        Nil
      )
    }
  }

  implicit def ObservationCountExtractor[T, S <: Predicate](implicit te: Extractor[T], pe: Extractor[S]): Extractor[(Observation[S], T)] = new Extractor[(Observation[S], T)] {
    def extract(v: JValue): (Observation[S], T) = {
      val where = (v \ "where" --> classOf[JObject])
      val count = (v \ "count").deserialize[T]

      (where.deserialize[Observation[S]](ObservationExtractor), count)
    }
  }

  implicit def ReportDecomposer[S <: Predicate, T](implicit tDecomposer: Decomposer[T], sDecomposer: Decomposer[S]): Decomposer[Report[S, T]] = new Decomposer[Report[S, T]] {
    def decompose(v: Report[S, T]): JValue = v.observationCounts.serialize
  }

  implicit def ReportExtractor[S <: Predicate, T](implicit aggregator: AbelianGroup[T], tExtractor: Extractor[T], sExtractor: Extractor[S]): Extractor[Report[S, T]] = new Extractor[Report[S, T]] {
    def extract(v: JValue): Report[S, T] = Report(v.deserialize[Map[Observation[S], T]])
  }
}
