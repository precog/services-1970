package com.reportgrid.examples.benchmark

import java.util.concurrent._

import com.reportgrid.analytics.Token
import com.reportgrid.api._
import com.reportgrid.api.blueeyes.ReportGrid

object AnalyticsBenchmark {
  def main(argv: Array[String]): Unit = {
    val benchmarkToken = config.getString("benchmarkToken", Token.Test.tokenId)
    val benchmarkUrl = config.getString("benchmarkUrl", "http://localhost:8888")

    val resultsToken = config.getString("resultsToken", Token.Test.tokenId)
    val resultsUrl = config.getString("resultsUrl", "http://localhost:8889")

    val testSystem = new ReportGrid(benchmarkToken, ReportGridConfig(benchmarkUrl))
    val resultsSystem = new ReportGrid(resultsToken, ReportGridConfig(resultsUrl))
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

case class SendTime(time: Long)
case class QueryTime(time: Long)

class AnalyticsBenchmark(testApi: ReportGrid, resultsApi: ReportGrid, conf: SamplingConfig) {

  def benchmark(rate: Long, samplesPerTest: Long): Unit = {
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
      loop {
        react {
          case Sample(sample) => 
            val start = System.nanoTime
            api.track(
              path       = "/benchmark",
              name       = "event",
              properties = sample,
              rollup     = false,
              timestamp  = clock.now(),
              count = Some(1)
            )

            resultsActor ! SampleTime(System.nanoTime - start)

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

        }
      }
    }

    def injector: Runnable = new Runnable {
      var remaining = conf.perRate
      override def run = {
        sampleActor ! Send
        remaining -= 1
        if (remaining <= 0) executorService.
      }
    }

    def sampler: Runnable = new Runnable {
      override def run = {
      }
    }

    val serviceexecutorService.scheduleAtFixedRate()
    done.await()
  }
}





// vim: set ts=4 sw=4 et:
