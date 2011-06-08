package com.reportgrid.analytics

case class Statistics(n: Long, min: Double, max: Double, mean: Double, variance: Double, standardDeviation: Double)

object Statistics {
	def zero = Statistics(0, 0, 0, 0, 0, 0)
}

case class RunningStats(min: Double, max: Double, sum: Double, sumSq: Double, n: Long) {
  def update(value: Double, n: Long): RunningStats = copy(
  	min   = this.min.min(value),
  	max   = this.max.max(value),
  	sum   = this.sum + (value * n),
  	sumSq = this.sumSq + n * (value * value),
  	n 	  = this.n + n
  )

  def statistics: Statistics = if (n == 0) Statistics.zero else {
  	val variance = (sumSq - ((sum * sum) / n)) / (n - 1)

  	Statistics(n = n, min = min, max = max, mean = sum / n, variance, standardDeviation = math.sqrt(variance))
  } 
}

object RunningStats {
  def zero = RunningStats(0, 0, 0, 0, 0L)
}
