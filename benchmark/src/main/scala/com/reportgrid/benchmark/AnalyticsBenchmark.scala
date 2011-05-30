package com.reportgrid
package benchmark

import java.util.concurrent._
import TimeUnit._

import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.util.CommandLineArguments
import blueeyes.util.Clock
import blueeyes.util.ClockSystem

import com.reportgrid.analytics.{Token, RunningStats}
import com.reportgrid.api._
import com.reportgrid.api.Series._
import com.reportgrid.api.blueeyes.ReportGrid
import scala.collection.mutable.ArrayBuffer

import net.lag.configgy.Configgy
import net.lag.configgy.Config

import scala.actors.Actor._
import org.scalacheck.Gen._

object AnalyticsBenchmark {
  def main(argv: Array[String]): Unit = {
    val args = CommandLineArguments(argv: _*)
    if (args.parameters.get("configFile").isDefined) {
      Configgy.configure(args.parameters.get("configFile").get)
      run(Configgy.config)
    } else {
      println("Usage: --configFile [filename]")
      println("Config file format:")
      println("""
        benchmarkToken = "your-token"
        benchmarkUrl = "http://benchmark-server/"
        resultsToken = "your-token"
        resultsUrl = "http://results-server/"
      """)
            
      System.exit(-1)
    }
  }

  def run(config: Config) = {
    val benchmarkToken = config.getString("benchmarkToken", Token.Test.tokenId)
    val benchmarkUrl = config.getString("benchmarkUrl", "http://localhost:8888")

    val resultsToken = config.getString("resultsToken", Token.Test.tokenId)
    val resultsUrl = config.getString("resultsUrl", "http://localhost:8889")

    val testSystem = new ReportGrid(benchmarkToken, ReportGridConfig(benchmarkUrl))
    val resultsSystem = new ReportGrid(resultsToken, ReportGridConfig(resultsUrl))

    val conf = new SamplingConfig {
        override val clock = ClockSystem.clockSystem
        override val maxRate = config.getLong("maxRate", 10000)
        override val samplesPerTestRate = config.getLong("samplesPerTestRate", 10000)
        override val sampleSet = new DistributedSampleSet(
          Vector(containerOfN[List, Int](100, choose(0, 50)).sample.get: _*),
          Vector(containerOfN[List, String](1000, for (i <- choose(3, 30); chars <- containerOfN[Array, Char](i, alphaChar)) yield new String(chars)).sample.get: _*),
          10
        )
    }

    val benchmark = new AnalyticsBenchmark(testSystem, resultsSystem, conf, System.out)
    benchmark.run()
  }
}

trait SamplingConfig {
  def clock: Clock
  def maxRate: Long
  def samplesPerTestRate: Long
  def sampleSet: SampleSet
}

trait SampleSet {
  // the number of samples to return in the queriablesamples list
  def queriableSampleSize: Int

  // Samples that have been injected and thus be used to construct 
  // queries that will return results. Returns None until the requisite
  // number of queriable samples has been reached.
  def queriableSamples: Option[List[JObject]]

  def next: (JObject, SampleSet)
}

case class DistributedSampleSet(sampleInts: Vector[Int], sampleStrings: Vector[String], queriableSampleSize: Int, queriableSamples: Option[List[JObject]] = None) extends SampleSet { self =>
  def gaussianIndex(size: Int): Int = {
    // multiplying by size / 5 means that 96% of the time, the sampled value will be within the range and no second try will be necessary
    val testIndex = (scala.util.Random.nextGaussian * (size / 5)) + (size / 2)
    if (testIndex < 0 || testIndex > size) gaussianIndex(size)
    else testIndex.toInt
  }

  def exponentialIndex(size: Int): Int = {
    import scala.math._
    round(exp(-random * 8) * size).toInt
  }

  def next = {
    val sample = JObject(
      JField("exp_str", sampleStrings(exponentialIndex(sampleStrings.size))) ::
      JField("exp_int", sampleInts(exponentialIndex(sampleInts.size))) ::
      JField("normal_str", sampleStrings(gaussianIndex(sampleStrings.size))) ::
      JField("normal_int", sampleInts(gaussianIndex(sampleInts.size))) ::
      JField("uniform_int", sampleInts(scala.util.Random.nextInt(sampleInts.size))) :: Nil
    )
    
    (sample, if (queriableSamples.exists(_.size >= queriableSampleSize)) this else this.copy(queriableSamples = Some(sample :: self.queriableSamples.getOrElse(Nil))))
  }
}

trait BenchmarkQuery {
  def interval: Long
  def query: JObject
}

case object Send
case object Query
case object Done
case class Sample(properties: JObject)
case class QueryFrom(samples: List[JObject])

case class TrackTime(time: Long)

sealed class QueryType
object QueryType {
  case object Count extends QueryType
  case object Series extends QueryType
  case object Search extends QueryType
  case object Intersect extends QueryType
  case object Histogram extends QueryType
}

case class QueryTime(queryType: QueryType, time: Long)

class AnalyticsBenchmark(testApi: ReportGrid, resultsApi: ReportGrid, conf: SamplingConfig, resultsStream: java.io.PrintStream) {
  def run(): Unit = {
		val rateStep = conf.maxRate / 5
		for (rate <- rateStep to conf.maxRate by rateStep) {
			benchmark(rate)
		}
  }

  def benchmark(rate: Long): Unit = {
    val startTime = conf.clock.now()
    val benchmarkPath = "/benchmark/" + startTime.toDate.getTime
    val benchmarkExecutor = Executors.newScheduledThreadPool(4)
    val resultsExecutor = Executors.newScheduledThreadPool(1)
    val done = new CountDownLatch(1)

    lazy val sampleActor = actor {
      var sampleSet = conf.sampleSet 
			loop {
				react {
					case Send =>
						val (sample, next) = sampleSet.next
						sampleSet = next
						sendActor ! Sample(sample)

					case Query => sampleSet.queriableSamples.foreach(samples => queryActor ! QueryFrom(samples))

					case Done => sendActor ! Done
				}
      }
    }

    lazy val sendActor = actor {
      var i = 0
      loop {
        react {
          case Sample(sample) => 
            val start = System.nanoTime
            testApi.track(
              path       = benchmarkPath,
              name       = "event",
              properties = sample,
              rollup     = false,
              timestamp  = Some(conf.clock.now().toDate),
              count = Some(1)
            )

            i += 1
            if (i % 10 == 0) {
              resultsActor ! TrackTime(System.nanoTime - start)
            }

          case Done => queryActor ! Done
        }
      }
    }

    lazy val queryActor = actor {
      loop {
        react {
          case QueryFrom(samples) =>
            val sampleKeys = (samples.flatMap {
              case JObject(fields) => fields map {
                case JField(name, _) => name
              }
            }).toSet

            //count
            time(testApi.select(Count).of(".track").from(benchmarkPath), QueryTime(QueryType.Count, _))

            //select
            time(testApi.select(Minute(Some((startTime.toDate, conf.clock.now().toDate)))).of(".track").from(benchmarkPath), QueryTime(QueryType.Series,_))

            //search
            //time(testApi.select(Minute(Some((startTime.toDate, clock.now().toDate)))).of(".track").from(benchmarkPath), QueryTime(QueryType.Search,_))

            //intersect
            if (sampleKeys.size > 1) {
							val k1 :: k2 :: Nil = sampleKeys.take(2).toList
							time(
								testApi.intersect(Count).top(20).of(k1).and.top(20).of(k2).from(benchmarkPath),
								QueryTime(QueryType.Intersect, _)
							)
            }

						//histogram
						//TODO


          case Done => resultsActor ! Done
        }
      }
    }

    lazy val resultsActor = actor {
      var trackStats = RunningStats.zero
      var queryStats = RunningStats.zero

      loop {
        react {
          case TrackTime(time) => 
            trackStats = trackStats.update(time, 1)
            resultsApi.track(
              path       = benchmarkPath,
              name       = "track",
              properties = JObject(List(JField("time", JInt(time)))),
              rollup     = false,
              count = Some(1)
            )

          case QueryTime(queryType, time) => 
            queryStats = queryStats.update(time, 1)
            resultsApi.track(
              path       = benchmarkPath + "/" + queryType,
              name       = "query",
              properties = JObject(List(JField("time", JInt(time)))),
              rollup     = false,
              count = Some(1)
            )

					case Done => 
						val ts = trackStats.statistics
						resultsStream.println("Tracking times:")
						resultsStream.println("min\tmax\tmean\tstddev")
						resultsStream.println(ts.min + "\t" + ts.max + "\t" + ts.mean + "\t" + ts.standardDeviation)

						val qs = queryStats.statistics
						resultsStream.println("Query times:")
						resultsStream.println("min\tmax\tmean\tstddev")
						resultsStream.println(qs.min + "\t" + qs.max + "\t" + qs.mean + "\t" + qs.standardDeviation)

						done.countDown()
        }
      }
    }

    def time[A](expr: => A, track: Long => Any): A = {
      val start = System.nanoTime()
      val result = expr
      resultsActor ! track(System.nanoTime() - start)
      result
    }

    def injector: Runnable = new Runnable {
      private var remaining = conf.samplesPerTestRate
      def run: Unit = {
        if (remaining <= 0) {
          sampleActor ! Done
          benchmarkExecutor.shutdown()
          resultsExecutor.shutdown()
        } else {
          sampleActor ! Send
          remaining -= 1
        }
      }
    }

    def sampler: Runnable = new Runnable {
      def run: Unit = {
        sampleActor ! Query
      }
    }

		resultsStream.println("Running benchmark at track rate of " + rate + " events/second")
    val benchmarkFuture = benchmarkExecutor.scheduleAtFixedRate(injector, 0, SECONDS.convert(1, NANOSECONDS) / rate, NANOSECONDS)
    val resultsFuture = resultsExecutor.scheduleAtFixedRate(sampler, 0, 1, SECONDS)

    done.await()
  }
}