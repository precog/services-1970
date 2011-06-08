package com.reportgrid.analytics

trait AbelianGroup[T] {
  def zero: T

  def inverse(v: T): T

  def aggregate(t1: T, t2: T): T
}

trait AggregatorImplicits {
  implicit val intAggregator = new AbelianGroup[Int] {
    def zero = 0

    def inverse(v: Int): Int = -v

    def aggregate(t1: Int, t2: Int): Int = t1 + t2
  }

  implicit val longAggregator = new AbelianGroup[Long] {
    def zero = 0L

    def inverse(v: Long): Long = -v

    def aggregate(t1: Long, t2: Long): Long = t1 + t2
  }

  implicit val floatAggregator = new AbelianGroup[Float] {
    def zero = 0.0F

    def inverse(v: Float): Float = -v

    def aggregate(t1: Float, t2: Float): Float = t1 + t2
  }

  implicit val doubleAggregator = new AbelianGroup[Double] {
    def zero = 0.0

    def inverse(v: Double): Double = -v

    def aggregate(t1: Double, t2: Double): Double = t1 + t2
  }

  implicit def ReportAggregator[S <: Predicate, T: AbelianGroup] = new AbelianGroup[Report[S, T]] {
    private val aggT = implicitly[AbelianGroup[T]]

    def zero = Report.empty[S, T]

    def inverse(v: Report[S, T]): Report[S, T] = Report(v.observationCounts.transform { (k, v) => aggT.inverse(v) })

    def aggregate(v1: Report[S, T], v2: Report[S, T]) = v1 + v2
  }

  implicit def timeSeriesAggregator[T](implicit aggregator: AbelianGroup[T]) = new AbelianGroup[TimeSeries[T]] {
    def zero = new TimeSeries[T](Map())

    def inverse(v: TimeSeries[T]): TimeSeries[T] = -v

    def aggregate(t1: TimeSeries[T], t2: TimeSeries[T]): TimeSeries[T] = t1 + t2
  }

  implicit def mapAggregator[K, V](implicit aggregator: AbelianGroup[V]) = new AbelianGroup[Map[K, V]] {
    def zero = Map[K, V]()

    def inverse(v: Map[K, V]): Map[K, V] = v.transform { (key, value) => aggregator.inverse(value) }

    def aggregate(t1: Map[K, V], t2: Map[K, V]): Map[K, V] = {
      val keys = t1.keys ++ t2.keys

      keys.foldLeft(Map[K, V]()) { (map, key) =>
        val value1 = t1.get(key).getOrElse(aggregator.zero)
        val value2 = t2.get(key).getOrElse(aggregator.zero)

        map + (key -> aggregator.aggregate(value1, value2))
      }
    }
  }
}
object AggregatorImplicits extends AggregatorImplicits
