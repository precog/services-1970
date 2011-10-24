package com.reportgrid.analytics

import blueeyes._
import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.persistence.mongo._
import blueeyes.concurrent.Future

import net.lag.configgy.{Config, ConfigMap}
import net.lag.logging.Logger

import scalaz._
import Scalaz._

object Console {
  def apply(file: java.io.File): Future[Console] = {
    apply((new Config()) ->- (_.loadFile(file.getPath)))
  }

  def apply(config: ConfigMap): Future[Console] = {
    val eventsdbConfig = config.configMap("services.analytics.v1.eventsdb")
    val eventsMongo = new RealMongo(eventsdbConfig)
    val eventsdb = eventsMongo.database(eventsdbConfig("database"))

    val indexdbConfig = config.configMap("services.analytics.v1.indexdb")
    val indexMongo = new RealMongo(indexdbConfig)
    val indexdb = indexMongo.database(indexdbConfig("database"))

    TokenManager(indexdb, "tokens").map { tokenManager => 
      Console(
        AggregationEngine.forConsole(config, Logger.get, eventsdb, indexdb),
        tokenManager
      )
    }
  }
}

case class Console(engine: AggregationEngine, tokenManager: TokenManager)

object ReaggregationTool {
  def main(argv: Array[String]) {
    val argMap = parseOptions(argv.toList, Map.empty)

    val analyticsConfig = argMap.get("--configFile").getOrElse {
      throw new RuntimeException("Analytics service configuration file must be specified with the --configFile option.")
    }

    val pauseLength = argMap.get("--pause").map(_.toInt).getOrElse(30)

    Console(new java.io.File(analyticsConfig)).foreach(reprocess(_, pauseLength))
  }

  def parseOptions(opts: List[String], optMap: Map[String, String]): Map[String, String] = opts match {
    case "--configFile" :: filename :: rest => parseOptions(rest, optMap + ("--configFile" -> filename))
    case "--pause"      :: seconds     :: rest => parseOptions(rest, optMap + ("--pause" -> seconds))
    case x :: rest => 
      println("Unrecognized option: " + x)
      parseOptions(rest, optMap)

    case Nil => optMap
  }

  def reprocess(console: Console, pause: Int) {
    import console.engine._
    eventsdb(selectAll.from(events_collection).where("reprocess" === true).limit(50)) flatMap { results => 
      Future(
        results.toSeq.map { jv => 
          aggregateFromStorage(console.tokenManager, jv --> classOf[JObject]) flatMap { 
            case Success(count) if count > 0 => eventsdb(update(events_collection).set(JPath("reprocess") set (false)).where("_id" === jv \ "_id"))
            case Failure(error) => throw new IllegalStateException("An error occurred reaggregating event data: " + compact(render(jv)))
          }
        }: _*
      ) deliverTo { _ => 
        Thread.sleep(pause * 1000)
        reprocess(console, pause)
      } ifCanceled {
        _.foreach(ex => ex.printStackTrace)
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
