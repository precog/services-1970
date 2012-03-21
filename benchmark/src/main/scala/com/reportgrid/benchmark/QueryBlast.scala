/**
 * Copyright 2012, ReportGrid, Inc.
 *
 * Created by dchenbecker on 1/15/12 at 7:25 AM
 */
package com.reportgrid.benchmark

import _root_.blueeyes.json.JsonAST.{JObject, JValue}
import _root_.blueeyes.concurrent.Future
import _root_.blueeyes.core.http._
import _root_.blueeyes.core.service.engines.HttpClientXLightWeb
import _root_.blueeyes.core.data.BijectionsChunkString._
import java.util.concurrent._
import java.util.Date
import java.lang.{Thread, Object}

object QueryBlast {
  import Queries._
  var count = 0
  var errors = 0
  var startTime = 0L
  var sum = 0l
  var min = Long.MaxValue
  var max = Long.MinValue
  
  val interval = 1000
  val intervalDouble = interval.toDouble
  
  val notifyLock = new Object

  var maxCount : Option[Int] = None

  def notifyError() {
    notifyLock.synchronized {
      errors += 1
    }
  }

  case class Stats(sum: Long, count: Long, min: Long, max: Long) {
    def update(time: Long) = Stats(sum + time, count + 1, math.min(min, time), math.max(max, time))
  }

  object Stats {
    def apply(t: Long) = new Stats(t, 1, t, t)
  }

  var statsMap = Map[String, Stats]()

  def notifyComplete(name: String, nanos : Long) {
    notifyLock.synchronized {
      count += 1
      
      val stats = statsMap.get(name) map {
        _.update(nanos)
      } getOrElse {
        Stats(nanos)
      }

      statsMap += (name -> stats)

      if ((count + errors) % interval == 0) {
        val now = System.currentTimeMillis()
        statsMap foreach { 
          case (name, stats) =>
            println("%-20d\t%12d\t%f\t%f\t%f\t%f\t%s".format(now, errors, intervalDouble / ((now - startTime) / 1000.0d), stats.min / 1000000.0d, stats.max / 1000000.0d, (stats.sum / stats.count) / 1000000.0d), name)
        }
        statsMap = Map[String, Stats]()
        startTime = now
      }
    }

    maxCount.foreach { mc => if (count >= mc) { println("Shutdown"); sys.exit() } }
  } 
  
  def main(args: Array[String]) {
    val threadCount = if(args.length > 0) args(0).toInt else 25
    val workQueue = new ArrayBlockingQueue[Query](1000)

    (1 to threadCount).foreach { id =>
      new Thread {
        implicit val config = testConfig
        val client = new HttpClientXLightWeb
        override def run() {
          while (true) {
            try {
              val query = workQueue.take()
              val started = System.nanoTime()
              val f: Future[HttpResponse[String]] = client.contentType(MimeTypes.application/MimeTypes.json)
                                                          .get(query.query(randomPath))
              val cdl = new CountDownLatch(1)
              f.map { 
                case HttpResponse(status, _, _, _)         => status == HttpStatus(HttpStatusCodes.OK) 
                case _                                     => false
              } orElse { false } deliverTo { b => 
                if(!b) notifyError()
                cdl.countDown 
              }
              cdl.await()
              notifyComplete(query.name, System.nanoTime() - started)
            } catch {
              case e => 
                notifyError()
            }
          }
        }
      }.start()
    }

    // Start injecting
    startTime = System.currentTimeMillis()
    println("time                \ttotal errors\tqueries/s\tmin (ms)\tmax (ms)\tavg (ms)\tname")
    while(true) {
      def fillQueue(i: Int = 0) {
        if(i < interval) {
          workQueue.put(randomQuery)
          fillQueue(i+1)
        }
      }
      fillQueue()
    }
  }
}

object Queries {
  private val random = new java.util.Random
  private val maxPaths = 1000

  def randomQuery(): Query = {
    queries(random.nextInt(queries.length))
  }

  def randomPath(): String = "test2" //"p%04d".format(random.nextInt(maxPaths))

  case class Config(baseurl: String, token: String)

  val testConfig = Config("http://stageapp01.reportgrid.com/services/analytics/v1","A3BC1539-E8A9-4207-BB41-3036EC2C6E6D")

  val queries = List(
    CountQuery,
    BrowseQuery
  ) 

  sealed trait Query {
    def name: String
    def query(path: String)(implicit config: Config): String

    def queryGlue(middle: String, params: Map[String, String] = Map.empty)(implicit config: Config): String = {
      var finalParams = params + ("tokenId" -> config.token)
      def paramString(params: Map[String, String]) = 
        if(params.isEmpty) {
          ""
        } else {
          "?" + params.map {
            case (k, v) => "%s=%s".format(k,v)
          } mkString("&")
        }
      "%s/%s%s".format(config.baseurl, middle, paramString(finalParams))
    }
  }

  case object CountQuery extends Query {
    def name = "count"
    def query(path: String)(implicit config: Config) = {
      queryGlue( "vfs/query/%s/count".format(path) )
    }
  }
  
  case object BrowseQuery extends Query {
    def name = "browse"
    def query(path: String)(implicit config: Config) = {
      queryGlue( "vfs/query/%s/".format(path) )
    }
  }

  case class BrowseTimeRangeQuery extends Query {
    def name = "browse_time_range"
    def query(path: String)(implicit config: Config) = {
      queryGlue( "vfs/query/%s/".format(path) )
    }
  }
}
