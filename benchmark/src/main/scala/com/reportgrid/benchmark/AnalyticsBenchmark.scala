package com.reportgrid
package benchmark

import java.util.concurrent._
import TimeUnit._

import _root_.blueeyes.json.JsonAST._
import _root_.blueeyes.json.JsonDSL._
import _root_.blueeyes.util.CommandLineArguments
import _root_.blueeyes.util.Clock
import _root_.blueeyes.util.ClockSystem
import rosetta.json.blueeyes._

import com.reportgrid.api._
import com.reportgrid.api.blueeyes._

import net.lag.configgy.Configgy
import net.lag.configgy.Config
import org.joda.time.DateTime

import scala.actors.Actor
import scala.actors.Actor._
import scala.collection.mutable.ArrayBuffer

import org.scalacheck.Gen._
import analytics.{Token, RunningStats}
import analytics.Periodicity.Minute

object AnalyticsTool {
  def main(argv: Array[String]): Unit = {
    val args = CommandLineArguments(argv: _*)
    if (args.parameters.get("configFile").isDefined) {
      Configgy.configure(args.parameters.get("configFile").get)
      Task(args.parameters.get("task").getOrElse("benchmark")).run(Configgy.config)
    } else {
      println("Usage: --task [benchmark | token] --configFile [filename]")
      println("Config file format:")
      println("""
        benchmarkToken = "your-token"
        benchmarkUrl = "http://benchmark-server/"
        benchmarkPath = "myPath"
        resultsToken = "your-token"
        resultsUrl = "http://results-server/"
        resultsPath = "benchmark-results"
        maxRate = 1000 # in samples/second
        samplesPerTestRate = 1000 # number of samples to send
      """)
            
      System.exit(-1)
    }
  }
}

sealed trait Task {
  def run(config: Config): Unit
}

object Task {
  def apply(s: String): Task = s match {
    case "benchmark" => BenchmarkTask
    case x => 
      println("Unknown task: " + x)
      sys.exit(1)
  }
}

case object BenchmarkTask extends Task {
  def run(config: Config) = {
    val realClock = ClockSystem.realtimeClock
    val startTime = realClock.now()
    val benchmarkToken = config.getString("benchmarkToken", Token.Test.tokenId)
    val benchmarkUrl = config.getString("benchmarkUrl", Server.Dev.analyticsRootUrl)

    val resultsToken = config.getString("resultsToken", Token.Test.tokenId)
    val resultsUrl = config.getString("resultsUrl", Server.Dev.analyticsRootUrl)
    val resultsStream = config.getString("outStream") match {
      case Some("/dev/null") => new java.io.PrintStream(new java.io.OutputStream {
        override def write(i: Int) = ()
      })

      case _ => System.out
    }

    val testSystem = BenchmarkApi(
      new ReportGridClient[JValue](ReportGridConfig(benchmarkToken, Server(benchmarkUrl), new HttpClientApache)),
      config.getString("benchmarkPath", "/benchmark/" + startTime.toDate.getTime)
    )

    val resultsSystem = BenchmarkApi(
      new ReportGridClient[JValue](ReportGridConfig(resultsToken, Server(resultsUrl), new HttpClientApache)),
      config.getString("resultsPath", "/benchmark-results")
    )

    val conf = BenchmarkConfig(
      clock = realClock,
      maxRate = config.getLong("maxRate", 1000),
      maxConcurrent = config.getInt("maxConcurrent", 100),
      samplesPerTestRate = config.getLong("samplesPerTestRate", 1000),
      sampleSet = new DistributedSampleSet(
        //Vector(containerOfN[List, Int](100, choose(0, 50)).sample.get: _*),
        //Vector(containerOfN[List, String](1000, for (i <- choose(3, 30); chars <- containerOfN[Array, Char](i, alphaChar)) yield new String(chars)).sample.get: _*),
        10
      ),
      reportIncremental = config.getBool("reportIncremental", false)
    )

    val benchmark = new AnalyticsBenchmark(testSystem, resultsSystem, conf, startTime, resultsStream)
    benchmark.run()
  }
}

case class BenchmarkApi(client: ReportGridClient[JValue], path: String)

case class BenchmarkConfig(
  clock: Clock,
  maxRate: Long,
  maxConcurrent: Int,
  samplesPerTestRate: Long,
  sampleSet: SampleSet,
  reportIncremental: Boolean
)

trait SampleSet {
  // the number of samples to return in the queriablesamples list
  def queriableSampleSize: Int

  // Samples that have been injected and thus be used to construct 
  // queries that will return results. Returns None until the requisite
  // number of queriable samples has been reached.
  def queriableSamples: Option[List[JObject]]

  def next: (JObject, SampleSet)
}

object AdSamples {
  val genders = List("male", "female")
  val ageRanges = List("0-17", "18-24", "25-36", "37-48", "49-60", "61-75", "76-130")
  val platforms = List("android", "iphone", "web", "blackberry", "other")
  val campaigns = for (i <- 0 to 30) yield "c" + i
  val eventNames = List("impression", "click", "conversion")

  val browsers = List("chrome", "ie", "firefox", "safari", "opera", "other")
  val envs = List("Win7", "WinVista", "WinXP", "MacOSX", "Linux", "iPad", "iPhone", "Android", "other")
  val rawLocations = List(
    List("usa", "colorado", "boulder"),
    List("usa", "colorado", "denver"),
    List("usa", "colorado", "colorado springs"),
    List("usa", "california", "los angeles"),
    List("usa", "california", "san francisco"),
    List("usa", "nevada", "las vegas"),
    List("italy", "lombaridia", "milano"),
    List("portugal", "beja", "serpa")
  )

  val rawKeywords = List(
    List("apple","pear","peach"),
    List("apple"),
    List("pear","preach"),
    List("peach"),
    List("kiwi"),
    List("lemon","orange"),
    List("orange"),
    List("pear","orange","kiwi")
  )

  def gaussianIndex(size: Int): Int = {
    // multiplying by size / 5 means that 96% of the time, the sampled value will be within the range and no second try will be necessary
    val testIndex = (scala.util.Random.nextGaussian * (size / 5)) + (size / 2)
    if (testIndex < 0 || testIndex >= size) gaussianIndex(size)
    else testIndex.toInt
  }

  def exponentialIndex(size: Int): Int = {
    import scala.math._
    round(exp(-random * 8) * size).toInt.min(size - 1).max(0)
  }

  def adSample() = JObject(
    JField("gender", oneOf(genders).sample) ::
    JField("platform", platforms(exponentialIndex(platforms.size))) ::
    JField("campaign", campaigns(gaussianIndex(campaigns.size))) ::
    JField("cpm", chooseNum(1, 100).sample) ::
    JField("ageRange", ageRanges(gaussianIndex(ageRanges.size))) :: Nil
  )

  def impressionSample() = JObject(List(
    JField("impression", JObject(List(
      JField("browser", oneOf(browsers).sample),
      JField("evn", oneOf(envs).sample),
      JField("gender", oneOf(genders).sample),
      JField("age", chooseNum(18,97).sample),
      JField("#location", location()),
      JField("keywords", keywords()),
      JField("#timestamp", timestamp())
    )))
  ))

  def keywords() = JObject( oneOf(rawKeywords).sample.get.map{ JField(_, true) } )

  def timestamp(): Long = {
    val random = new java.util.Random
    val now = new DateTime()
    val start = now.minusMonths(1)
    val finish = now.plusMonths(1)
    val range = (finish.getMillis - start.getMillis).toInt
    val offset = random.nextInt(range)
    start.getMillis + offset
  }

  def location() = {
    val loc = oneOf(rawLocations).sample
    JObject(List(
      JField("country", loc(0)),
      JField("state", loc.take(2).mkString("/")),
      JField("city", loc.mkString("/"))
    ))
  }
}

case class DistributedSampleSet(queriableSampleSize: Int, queriableSamples: Option[List[JObject]] = None, sampler: () => JObject = AdSamples.adSample _) extends SampleSet { self =>
  import AdSamples._
  def next = {
    val sample = sampler() 
    
    (sample, if (queriableSamples.exists(_.size >= queriableSampleSize)) this else new DistributedSampleSet(queriableSampleSize, Some(sample :: self.queriableSamples.getOrElse(Nil)), sampler))
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
case class Ready(actor: Actor)


sealed trait QueryType
object QueryType {
  case object Count extends QueryType
  case object Series extends QueryType
  case object Search extends QueryType
  case object Intersect extends QueryType
  case object Histogram extends QueryType
}

sealed trait Timing
case class TrackTime(time: Long) extends Timing
case class QueryTime(queryType: QueryType, time: Long) extends Timing

class AnalyticsBenchmark(testApi: BenchmarkApi, resultsApi: BenchmarkApi, conf: BenchmarkConfig, startTime: DateTime, resultsStream: java.io.PrintStream) {
  import AdSamples._

  def run() {
		val rateStep = conf.maxRate / 5
		for (rate <- rateStep to conf.maxRate by rateStep) {
			benchmark(rate, conf.maxConcurrent)
		}
  }

  def benchmark(rate: Long, maxConcurrent: Int) {
    val benchmarkExecutor = Executors.newScheduledThreadPool(20)
    val resultsExecutor = Executors.newScheduledThreadPool(1)
    val done = new CountDownLatch(1)

    lazy val sampleActor = actor {
      var sampleSet = conf.sampleSet 
			loop {
				react {
					case Send =>
						val (sample, next) = sampleSet.next
						sampleSet = next
					  sendCoordinator ! Sample(sample)

					case Query => 
            sampleSet.queriableSamples.foreach(samples => queryActor ! QueryFrom(samples))

					case Done => 
            sendCoordinator ! Done
            exit
				}
      }
    }

    def sendActor(n: Int) = actor {
      loop {
        react {
          case Ready(sender) => sender ! Ready(self)
          case Sample(sample) => 
            val start = System.nanoTime
            try {
              println("Tracking from " + n)
              testApi.client.track(
                path       = testApi.path,
                name       = eventNames(exponentialIndex(eventNames.size)),
                properties = sample,
                rollup     = FullRollup,
                //timestamp  = Some(conf.clock.now().toDate),
                headers    = Map("User-Agent" -> "ReportGridBenchmark")
              )

              resultsActor ! TrackTime(System.nanoTime - start)
            } catch {
              case t: Throwable => t.printStackTrace()
            }

          case Done => 
            queryActor ! Done
            sys.exit()
        }
      }
    }

    lazy val sendActors = (0 to maxConcurrent).map(sendActor)

    lazy val sendCoordinator = actor {
      loop {
        react {
          case sample @ Sample(_) => 
            sendActors.foreach{_ ! Ready(self)}
            react {
              case Ready(sendActor) => sendActor ! sample
            }

          case Done =>
            sendActors.foreach{_ ! Done}
            sys.exit()
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

            try {
              import testApi.client.{Count,Series}
              //count
              time(testApi.client.select(Count).of(".click.gender").from(testApi.path), QueryTime(QueryType.Count, _))

              //select
              time(testApi.client.select(Series.Minute(startTime.toDate, conf.clock.now().toDate)).of(".click.gender").from(testApi.path), QueryTime(QueryType.Series,_))

              //search
              //time(testApi.client.select(Minute(Some((startTime.toDate, clock.now().toDate)))).of(".click.gender").from(testApi.path), QueryTime(QueryType.Search,_))

              //intersect
              if (sampleKeys.size > 1) {
                val k1 :: k2 :: Nil = sampleKeys.take(2).toList
                time(
                    testApi.client.intersect(Count).top(20).of(k1).and.top(20).of(k2).from(testApi.path),
                    QueryTime(QueryType.Intersect, _)
                )
              }

              //histogram
              //TODO
            } catch {
              case t: Throwable => t.printStackTrace()
            }

          case Done => 
            resultsActor ! Done
            sys.exit()
        }
      }
    }

    lazy val resultsActor = actor {
      var trackStats = RunningStats.zero
      var queryStats = RunningStats.zero

      def printStats = {
        val ts = trackStats.statistics
        resultsStream.println("\t\tn\tmin\tmax\tmean\tstddev")
        resultsStream.print("Tracking times:\t")
        resultsStream.println(ts.n + "\t" +
                              MILLISECONDS.convert(ts.min.toLong,  NANOSECONDS) + "\t" + 
                              MILLISECONDS.convert(ts.max.toLong,  NANOSECONDS) + "\t" + 
                              MILLISECONDS.convert(ts.mean.toLong, NANOSECONDS) + "\t" + 
                              MILLISECONDS.convert(ts.standardDeviation.toLong, NANOSECONDS))

        val qs = queryStats.statistics
        resultsStream.print("Query times:\t")
        resultsStream.println(qs.n + "\t" +
                              MILLISECONDS.convert(qs.min.toLong,  NANOSECONDS) + "\t" + 
                              MILLISECONDS.convert(qs.max.toLong,  NANOSECONDS) + "\t" + 
                              MILLISECONDS.convert(qs.mean.toLong, NANOSECONDS) + "\t" + 
                              MILLISECONDS.convert(qs.standardDeviation.toLong, NANOSECONDS))
      }

      loop {
        react {
          case TrackTime(time) => 
            trackStats = trackStats.update(time, 1)
            resultsApi.client.track(
              path       = resultsApi.path,
              name       = "track",
              properties = JObject(List(JField("time", JInt(time)))),
              rollup     = NoRollup,
              count = Some(1)
            )

          case QueryTime(queryType, time) => 
            queryStats = queryStats.update(time, 1)
            resultsApi.client.track(
              path       = resultsApi.path + "/" + queryType.toString.toLowerCase,
              name       = "query",
              properties = JObject(List(JField("time", JInt(time)))),
              rollup     = NoRollup,
              count = Some(1)
            )

            if (conf.reportIncremental && queryStats.statistics.n % 10 == 0) printStats

					case Done =>
            printStats
						done.countDown()
            sys.exit()
        }
      }
    }

    def time[A](expr: => A, track: Long => Timing): Option[A] = {
      val start = System.nanoTime()
      try {
        val result = expr
        resultsActor ! track(System.nanoTime() - start)
        Some(result)
      } catch {
        case t: Throwable => t.printStackTrace; None
      }
    }

    def injector: Runnable = new Runnable {
      private var remaining = conf.samplesPerTestRate
      def run() {
        if (remaining <= 0 && conf.samplesPerTestRate > 0) {
          println("Finished with injection")
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
      def run() {
        sampleActor ! Query
      }
    }

		resultsStream.println("Running benchmark at track rate of " + rate + " events/second with 2 queries/second")
    val benchmarkFuture = benchmarkExecutor.scheduleAtFixedRate(injector, 0, NANOSECONDS.convert(1, SECONDS) / rate, NANOSECONDS)
//    val resultsFuture = resultsExecutor.scheduleAtFixedRate(sampler, 0, 500, MILLISECONDS)

    done.await()
  }
}
