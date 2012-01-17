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

object TrackBlast {
  var count = 0
  var startTime = 0L
  var sum = 0l
  var min = Long.MaxValue
  var max = Long.MinValue
  
  val interval = 10000
  val intervalDouble = interval.toDouble
  
  val notifyLock = new Object

  def notifyComplete(nanos : Long) {
    notifyLock.synchronized {
      count += 1
      sum += nanos
      min = math.min(min, nanos)
      max = math.max(max, nanos)

      if (count % interval == 0) {
        val now = System.currentTimeMillis()
        println("%s,%f,%f,%f,%f".format(new Date, intervalDouble / ((now - startTime) / 1000.0d), min / 1000000.0d, max / 1000000.0d, (sum / intervalDouble) / 1000000.0d))
        startTime = now
        sum = 0l
        min = Long.MaxValue
        max = Long.MinValue
      }
    }
  } 
  
  def main(args: Array[String]) {
    val apiUrl = args match {
      case Array(url) => Server(url)
      case _ => Server.Dev
    }
    
    val sampleSet = new DistributedSampleSet(10)

    val workQueue = new ArrayBlockingQueue[JObject](1000)

//    println("Starting workers")
    
    (1 to 100).foreach { id =>
      new Thread {
        val client = new ReportGridClient[JValue](ReportGridConfig(Token.Test, apiUrl, new HttpClientApache))
        val path = "/benchmark/" + id

        override def run() {
          import AdSamples._
          while (true) {
            val sample = workQueue.take()
            val started = System.nanoTime()
            client.track(path,
                         eventNames(exponentialIndex(eventNames.size)),
                         properties = sample,
                         rollup     = true,
                         //timestamp  = Some(conf.clock.now().toDate),
                         headers    = Map("User-Agent" -> "ReportGridBenchmark"))
            notifyComplete(System.nanoTime() - started)
          }
        }
      }.start()
    }

    // Start injecting
    startTime = System.currentTimeMillis()
    //println("Starting sample inject")
    println("time,tracks/s,min (ms),max (ms),avg (ms)")
    while(true) {
      val (sample, next) = sampleSet.next
      workQueue.put(sample)
    }
  }
}