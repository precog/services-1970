package com.reportgrid.analytics.persistence

import blueeyes.json.JsonAST._
import blueeyes.json.{JPath, JPathNode, JPathField, JPathIndex, JPathImplicits}
import blueeyes.json.JsonParser.{parse}
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._

import blueeyes.persistence.mongo._

import blueeyes.util.SpecialCharTranscoder

import com.reportgrid.analytics._

import org.joda.time.{DateTime, DateTimeZone}

/** Support for persitence via MongoDB.
 */
object MongoSupport {
  val MongoEscaper = SpecialCharTranscoder.fromMap('/',
    Map(
      '.' -> '*',
      '$' -> 'S'
    )
  )

  implicit val JPathNodeDecomposer = new Decomposer[JPathNode] {
    def decompose(v: JPathNode): JValue = v.toString.serialize
  }

  implicit val JPathNodeExtractor = new Extractor[JPathNode] {
    def extract(v: JValue): JPathNode = {
      val string = v.deserialize[String]

      JPath(string).nodes match {
        case node :: Nil => node

        case _ => error("Too many or few nodes to extract JPath node from " + v)
      }
    }
  }

  implicit val JPathDecomposer: Decomposer[JPath] = new Decomposer[JPath] {
    def decompose(v: JPath): JValue = v.toString.serialize
  }

  implicit val JPathExtractor: Extractor[JPath] = new Extractor[JPath] {
    def extract(v: JValue): JPath = JPath(v.deserialize[String])
  }

  implicit def IntUpdater(jpath: JPath, value: Int): MongoUpdate = jpath inc value

  implicit def LongUpdater(jpath: JPath, value: Long): MongoUpdate = jpath inc value

  implicit def FloatUpdater(jpath: JPath, value: Float): MongoUpdate = jpath inc value

  implicit def DoubleUpdater(jpath: JPath, value: Double): MongoUpdate = jpath inc value

  implicit def TimeSeriesUpdater[T](jpath: JPath, value: TimeSeries[T])(implicit updater: (JPath, T) => MongoUpdate): MongoUpdate = {
    value.series.foldLeft[MongoUpdate](MongoUpdateNothing) {
      case (fullUpdate, (period, count)) =>
        fullUpdate & updater(jpath \ period.periodicity.name \ period.start.getMillis.toString, count)
    }
  }

  implicit val DateTimeExtractor = new Extractor[DateTime] {
    def extract(jvalue: JValue): DateTime = new DateTime(jvalue.deserialize[Long], DateTimeZone.UTC)
  }

  implicit val DateTimeDecomposer = new Decomposer[DateTime] {
    def decompose(dateTime: DateTime): JValue = JInt(dateTime.getMillis)
  }

  implicit val PeriodicityExtractor = new Extractor[Periodicity] {
    def extract(value: JValue): Periodicity = Periodicity(value.deserialize[String])
  }

  implicit val PeriodicityDecomposer = new Decomposer[Periodicity] {
    def decompose(periodicity: Periodicity): JValue = periodicity.name
  }

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
      (value \ "start").deserialize[DateTime]
    )
  }

  implicit def TimeSeriesDecomposer[T](implicit decomposer: Decomposer[T]) = new Decomposer[TimeSeries[T]] {
    def decompose(value: TimeSeries[T]): JValue = JObject(value.groupByPeriodicity.toList.map { tuple =>
      val (periodicity, timeSeries) = tuple

      JField(periodicity.name,
        (timeSeries.series.map { (tuple) =>
          val (period, count) = tuple

          (period.start.getMillis.toString, count)
        }: Map[String, T]).serialize
      )
    })
  }

  implicit def TimeSeriesExtractor[T](implicit aggregator: Aggregator[T], extractor: Extractor[T]) = new Extractor[TimeSeries[T]] {
    def extract(value: JValue): TimeSeries[T] = value match {
      case JObject(fields) =>
        TimeSeries[T](
          fields.foldLeft(Map.empty[Period, T]) { (map, field) =>
            val periodicity = Periodicity(field.name)

            map ++ field.value.deserialize[Map[String, T]].map { tuple =>
              val (timeString, count) = tuple

              (Period(periodicity, new DateTime(timeString.toLong, DateTimeZone.UTC)), count)
            }
          }
        )

      case _ => error("Expected object but found: " + value)
    }
  }

  /** Serializes HasChild Predicate into a JValue.
   */
  implicit val HasChildDecomposer = new Decomposer[HasChild] {
    def decompose(v: HasChild): JValue = v.child.serialize
  }

  implicit val HasChildExtractor = new Extractor[HasChild] {
    def extract(v: JValue): HasChild = HasChild(v.deserialize[JPathNode])
  }

  implicit val HasValueDecomposer = new Decomposer[HasValue] {
    def decompose(v: HasValue): JValue = v.value
  }

  implicit val HasValueExtractor = new Extractor[HasValue] {
    def extract(v: JValue): HasValue = HasValue(v)
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

  implicit def ReportDecomposer[T, S <: Predicate](implicit tDecomposer: Decomposer[T], sDecomposer: Decomposer[S]): Decomposer[Report[T, S]] = new Decomposer[Report[T, S]] {
    def decompose(v: Report[T, S]): JValue = v.observationCounts.serialize
  }

  implicit def ReportExtractor[T, S <: Predicate](implicit aggregator: Aggregator[T], tExtractor: Extractor[T], sExtractor: Extractor[S]): Extractor[Report[T, S]] = new Extractor[Report[T, S]] {
    def extract(v: JValue): Report[T, S] = Report(v.deserialize[Map[Observation[S], T]])
  }

  implicit val StatisticsDecomposer = new Decomposer[Statistics] {
    def decompose(v: Statistics): JValue = JObject(
      JField("n",  v.n.serialize) ::
      JField("min",  v.min.serialize) ::
      JField("max",  v.max.serialize) ::
      JField("mean", v.mean.serialize) ::
      JField("variance", v.variance.serialize) ::
      JField("standardDeviation", v.standardDeviation.serialize) ::
      Nil
    )
  }

  implicit val StatisticsExtractor = new Extractor[Statistics] {
    def extract(v: JValue): Statistics = Statistics(
      n = (v \ "n").deserialize[Long],
      min = (v \ "min").deserialize[Double],
      max = (v \ "max").deserialize[Double],
      mean = (v \ "mean").deserialize[Double],
      variance = (v \ "variance").deserialize[Double],
      standardDeviation = (v \ "standardDeviation").deserialize[Double]
    )
  }
}
