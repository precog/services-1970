package com.reportgrid.examples.benchmark

import java.util.concurrent._
import TimeUnit._

import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.util.CommandLineArguments

import com.reportgrid.analytics.Token
import com.reportgrid.api._
import com.reportgrid.api.blueeyes.ReportGrid

import net.lag.configgy.Configgy
import net.lag.configgy.Config

import scala.actors.Actor._

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

    val benchmark = new AnalyticsBenchmark(testSystem, resultsApi, conf) 
    benchmark.run()
  }
}

trait SamplingConfig {
  def clock: Clock
  def maxRate: Long
  def perRate: Long
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

trait BenchmarkQuery {
  def interval: Long
  def query: JObject
}

case object Send
case object Query
case object Done
case class Sample(rate: Long, properties: JObject)
case class QueryFrom(rate: Long, samples: List[JObject])

case class TrackTime(time: Long)
case class QueryTime(time: Long)

class AnalyticsBenchmark(testApi: ReportGrid, resultsApi: ReportGrid, conf: SamplingConfig) {
  def run(): Unit = {
  }

  def benchmark(rate: Long, timeUnit: TimeUnit, samplesPerTest: Long): Unit = {
    val benchmarkExecutor = Executors.newScheduledThreadPool(4)
    val resultsExecutor = Executors.newScheduledThreadPool(1)
    val done = new CountDownLatch(1)

    val sampleActor = actor {
      var sampleSet = conf.sampleSet 
      receive {
        case Send => 
          val (sample, next) = sampleSet.next
          sampleSet = next
          sendActor ! Sample(sample)

        case Query => sampleSet.queriableSamples.foreach {
          samples => queryActor ! QueryFrom(samples)
        }

        case Done => sendActor ! Done
      }
    }

    val sendActor = actor {
      var i = 0
      loop {
        react {
          case Sample(sample) => 
            val start = System.nanoTime
            testApi.track(
              path       = "/benchmark",
              name       = "event",
              properties = sample,
              rollup     = false,
              timestamp  = clock.now(),
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

    val queryActor = actor {
      loop {
        react {
          case QueryFrom(samples) =>
            val start = System.nanoTime


          case Done => done.countDown()
        }
      }
    }

    val resultsActor = actor {
      loop {
        react {
          case TrackTime(time) => 
            resultsApi.track(
              path       = "/benchmark",
              name       = "track",
              properties = JObject(List(JField("time", JLong(time))))
              rollup     = false,
              timestamp  = clock.now(),
              count = Some(1)
            )

          case QueryTime(time) => 
            resultsApi.track(
              path       = "/benchmark",
              name       = "query",
              properties = JObject(List(JField("time", JLong(time))))
              rollup     = false,
              timestamp  = clock.now(),
              count = Some(1)
            )
        }
      }
    }

    def injector: Runnable = new Runnable {
      var remaining = conf.perRate
      override def run = {
        if (remaining <= 0) {
          sampleActor ! Done
          benchmarkFuture.shutdown()
          resultsFuture.shutdown()
        } else {
          sampleActor ! Send
          remaining -= 1
        }
      }
    }

    def sampler: Runnable = new Runnable {
      override def run = {
        sampleActor ! Query
      }
    }


    val benchmarkFuture = benchmarkExecutor.scheduleAtFixedRate(injector, 0, SECONDS.convert(1, NANOSECONDS) / rate, NANOSECONDS)
    val resultsFuture = resultsExecutor.scheduleAtFixedRate(sampler, 0, 1, SECONDS)

    done.await()
  }
}





// vim: set ts=4 sw=4 et:
