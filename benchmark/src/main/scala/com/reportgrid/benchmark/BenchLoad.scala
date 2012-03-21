/**
 * Copyright 2012, ReportGrid, Inc.
 *
 * Created by dchenbecker on 1/15/12 at 7:25 AM
 */
package com.reportgrid.benchmark

import _root_.blueeyes.json.JsonAST.{JObject, JValue}
import com.reportgrid.api._
import java.util.concurrent.ArrayBlockingQueue
import java.lang.{Thread, Object}
import rosetta.json.blueeyes._
import java.util.Date

object BenchLoad {
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
  var threadCount : Option[Int] = None 
    
  val workQueue = new ArrayBlockingQueue[(JObject, Int)](10000)

  def notifyError() {
    notifyLock.synchronized {
      errors += 1
    }
  }

  def notifyComplete(nanos : Long) {
    notifyLock.synchronized {
      count += 1
      sum += nanos
      min = math.min(min, nanos)
      max = math.max(max, nanos)

      if ((count + errors) % interval == 0) {
        val now = System.currentTimeMillis()
        println("%-20d\t%12d\t%f\t%f\t%f\t%f".format(now, errors, intervalDouble / ((now - startTime) / 1000.0d), min / 1000000.0d, max / 1000000.0d, (sum / intervalDouble) / 1000000.0d))
        startTime = now
        sum = 0l
        min = Long.MaxValue
        max = Long.MinValue
      }
    }
    
    maxCount.foreach { mc => if (count >= mc) { println("Shutdown"); sys.exit() } }
  } 
  
  val sampleSet = new DistributedSampleSet(10)
  val pathCount = 100
  
  def main(args: Array[String]) {
    val apiUrl = args match {
      case Array(url) => Server(url)
      case Array(url, threadCountArg) => 
        println("ThreadCount = " + threadCountArg); threadCount = Some(threadCountArg.toInt); Server(url)
      case Array(url, threadCountArg, maxSamples) => 
        println("ThreadCount = " + threadCountArg + " Max = " + maxSamples); threadCount = Some(threadCountArg.toInt); maxCount = Some(maxSamples.toInt); Server(url)
      case _ => Server.Dev
    }
    
    startTime = System.currentTimeMillis()

    val threads = (1 to threadCount.get).map { threadId =>
      new Thread() {
        val client = new ReportGridClient[JValue](ReportGridConfig(Token.Test, apiUrl, new HttpClientApache))
        override def run() {
          def insert(cnt: Int = 0): Unit = {
            try {
              val started = System.nanoTime()
              val (query, pathId) = workQueue.take() 
              val path = "/stress/test%09d".format(pathId)
              client.track(path,
                           "impression",
                           properties = query,
                           rollup     = FullRollup,
                           headers    = Map("User-Agent" -> "ReportGridBenchmark"))
              notifyComplete(System.nanoTime() - started)
            } catch {
              case e => notifyError()
            }
            if(threadId % 10 == 0 && cnt % 10 == 0) {
              println("Insert progress: " + threadId + "-" + cnt)
            }
            insert(cnt + 1)
          }

          insert()
        }
      }
    }
    threads.foreach( _.start )
    
    while(true) {
      def loadQueue(i: Int = 0): Unit = {
        val (sample, _) = sampleSet.next
        workQueue.put((sample, i))
        if(i < pathCount) {
          loadQueue(i+1)
        } else {
          ()
        }
      }
      loadQueue()
    }
    
    threads.foreach( _.join )
  }
 
}
