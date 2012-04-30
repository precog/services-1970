package com.reportgrid.analytics

import akka.actor.PoisonPill
import blueeyes._
import blueeyes.bkka._
import blueeyes.health._
import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.JodaSerializationImplicits._
import blueeyes.persistence.mongo._
import blueeyes.concurrent.Future
import blueeyes.concurrent.Future._
import blueeyes.concurrent.ScheduledExecutorSingleThreaded
import blueeyes.util.metrics.Duration._

import java.util.concurrent.CountDownLatch
import net.lag.configgy.{Config, ConfigMap}
import com.weiglewilczek.slf4s.Logging
import com.weiglewilczek.slf4s.Logger
import org.joda.time.Instant

import scalaz.{Success, Failure, NonEmptyList}
import scalaz.Scalaz._

object AggregationEnvironment {
  def apply(file: java.io.File): Future[AggregationEnvironment] = {
    apply((new Config()) ->- (_.loadFile(file.getPath)))
  }

  def apply(config: ConfigMap): Future[AggregationEnvironment] = {
    val eventsdbConfig = config.configMap("services.analytics.v1.eventsdb")

    val insertEventsMongo = new RealMongo(eventsdbConfig)
    val insertEventsdb = insertEventsMongo.database(eventsdbConfig("database"))

    val queryEventsMongo = new RealMongo(eventsdbConfig)
    val queryEventsdb = queryEventsMongo.database(eventsdbConfig("database"))
   
    val indexdbConfig = config.configMap("services.analytics.v1.indexdb")
    val indexMongo = new RealMongo(indexdbConfig)
    val indexdb = indexMongo.database(indexdbConfig("database"))

    val tokensCollection = config.getString("tokens.collection", "tokens")
    val deletedTokensCollection = config.getString("tokens.deleted", "deleted_tokens")

    implicit val shutdownTimeout = akka.actor.Actor.Timeout(indexdbConfig.getLong("shutdownTimeout", Long.MaxValue))

    val tokenManager = new TokenManager(indexdb, tokensCollection, deletedTokensCollection) 
    val engine = AggregationEngine.forConsole(config, Logger("reaggregator"), insertEventsdb, queryEventsdb, indexdb, HealthMonitor.Noop)
    val stoppable = Stoppable(engine, 
      Stoppable(insertEventsdb, Stoppable(insertEventsMongo) :: Nil) :: 
      Stoppable(queryEventsdb, Stoppable(queryEventsMongo) :: Nil) :: 
      Stoppable(indexdb, Stoppable(indexMongo) :: Nil) :: Nil)

    Future.sync(new AggregationEnvironment(engine, tokenManager, stoppable, shutdownTimeout))
  }
}

case class AggregationEnvironment(engine: AggregationEngine, tokenManager: TokenStorage, stoppable: Stoppable, timeout: akka.actor.Actor.Timeout)

object ReaggregationTool extends Logging {
  def halt = {
    akka.actor.Actor.registry.actors.foreach(_ ! PoisonPill)
    logger.info("Exiting.")
    System.exit(0)
  }

  def main(argv: Array[String]) {
    val argMap = parseOptions(argv.toList, Map.empty)

    val analyticsConfig = argMap.get("--configFile").getOrElse {
      throw new RuntimeException("Analytics service configuration file must be specified with the --configFile option.")
    }

    val pauseLength = argMap.get("--pause").map(_.toInt).getOrElse(30)
    val batchSize   = argMap.get("--batchSize").map(_.toInt).getOrElse(50)
    val maxRecords  = argMap.get("--maxRecords").map(_.toLong).getOrElse(5000L)

    val shutdownLatch = new java.util.concurrent.CountDownLatch(1)

    def onTimeout[T](future: akka.dispatch.Future[T]) = {
      logger.info("Shutdown future timed out after " + (future.timeoutInNanos / 1000 / 1000) + " ms.")
      shutdownLatch.countDown()
    }

    val startTime = System.currentTimeMillis
    val workFuture = for {
      env         <- AggregationEnvironment(new java.io.File(analyticsConfig))
      totalEvents <- reprocess(env.engine, env.tokenManager, pauseLength, batchSize, maxRecords)
      stopResult  <- Stoppable.stop(env.stoppable)(env.timeout).onTimeout(onTimeout).toBlueEyes
    } yield {
      val stopTime = System.currentTimeMillis
      logger.info("Finished processing " + totalEvents + " events in " + ((stopTime - startTime) / 1000d) + " seconds.")
      shutdownLatch.countDown()
    } 

    workFuture ifCanceled { ex =>
      ex match {
        case Some(throwable) => logger.error("Reaggregation was halted due to an error", throwable)
        case None => logger.error("Reggregation future was canceled, but no reason was given.")
      }

      shutdownLatch.countDown()
    }
    
    shutdownLatch.await()
    halt
  }

  def parseOptions(opts: List[String], optMap: Map[String, String]): Map[String, String] = opts match {
    case "--configFile" :: filename :: rest => parseOptions(rest, optMap + ("--configFile" -> filename))
    case "--pause"      :: seconds  :: rest => parseOptions(rest, optMap + ("--pause" -> seconds))
    case "--batchSize"  :: records  :: rest => parseOptions(rest, optMap + ("--batchSize" -> records))
    case "--maxRecords" :: records  :: rest => parseOptions(rest, optMap + ("--maxRecords" -> records))
    case x :: rest => 
      println("Unrecognized option: " + x)
      parseOptions(rest, optMap)

    case Nil => optMap
  }

  def reprocess(engine: AggregationEngine, tokenManager: TokenStorage, pause: Int, batchSize: Int, maxRecords: Long) = {
    import engine.{logger => _, _}

    logger.info("Beginning processing " + maxRecords + " events.")
    val ingestBatch: Function0[Future[Int]] = () => {
      queryEventsdb(selectAll.from(events_collection).where("reprocess" === true).sortBy(".timestamp".<<).limit(batchSize)) flatMap { results => 
        logger.info("Reaggregating...")
        val reaggregated = results.toList.zipWithIndex.map { case (jv, i) => restore(engine, tokenManager, jv --> classOf[JObject], i) }

        Future(reaggregated.toSeq: _*) map { results => 
          val (errors, complexity) = results.foldLeft((List.empty[String], 0L)) {
            case ((errors, total), Failure(err)) => (errors ++ err.list, total)
            case ((errors, total), Success(complexity)) => (errors, total + complexity)
          }

          logger.info("\nProcessed " + results.size + " events with a total complexity of " + complexity)
          print(": " + complexity + "\n\n")
          errors.foreach(logger.warn(_))

          results.size
        } ifCanceled {
          errors => 
            errors.foreach(logger.error("Fatal reprocessing error.", _)) 
            logger.error("Errors caused event reprocessing to be terminated.")
        } flatMap { size =>
          logger.debug("About to flush aggregation engine stages...")
          engine.flushStages.map { writes => 
            logger.debug("Flushed " + writes + " writes to mongo.")
            size
          }
        }
      }
    }

    ScheduledExecutorSingleThreaded.repeatWhile(ingestBatch, pause.seconds, (_: Int) < maxRecords)(0) { (_: Int) + (_: Int) } 
  }

  def restore(engine: AggregationEngine, tokenManager: TokenStorage, obj: JObject, i: Int): Future[ValidationNEL[String, Long]] = {
    print("*")
    import engine.{aggregate, insertEventsdb, events_collection, withTagResults}
    tokenManager.lookup(obj \ "token") flatMap { 
      _ map { token => 
        logger.trace("obtained token " + i)
        ((obj \ "event" \ "data") -->? classOf[JObject]) map { eventBody =>
          val path = (obj \ "path").deserialize[Path]
          val count = (obj \ "count").deserialize[Int]
          val timestamp = (obj \ "timestamp").deserialize[Instant]
          val eventName = (obj \ "event" \ "name").deserialize[String]

          val tagExtractors = Tag.timeTagExtractor(AggregationEngine.timeSeriesEncoding, timestamp, false) ::
                              Tag.locationTagExtractor(Future.sync(None))      :: Nil

          val (tagResults, remainder) = Tag.extractTags(tagExtractors, eventBody --> classOf[JObject])
          withTagResults(tagResults) { tags =>
            logger.trace("about to aggregate " + i)
            aggregate(token, path, eventName, tags, remainder, count)
            .deliverTo { complexity => logger.trace("finished aggregation with complexity of " + complexity + " " + i) } 
            .map { complexity => (complexity, tags) }
          } 
        } getOrElse {
          Future.sync(("Event body was not a jobject: " + (obj \ "data")).wrapNel.fail)
        }
      } getOrElse {
        Future.sync(("No token found for tokenId: " + (obj \ "token")).wrapNel.fail)
      } map {
        case Success((complexity, tags))  => 
          (obj \ "tags") match {
            case JNothing => (complexity.success[NonEmptyList[String]], (JPath("reprocess") set (false)) |+| (JPath("tags") set (tags.serialize)))
            case _        => (complexity.success[NonEmptyList[String]], (JPath("reprocess") set (false)))
          }

        case Failure(error) => 
          (error.fail[Long], (JPath("reprocess") set (false)) |+| (JPath("unparseable") set (true)))

      } flatMap {
        case (complexity, mongoUpdate) => 
          print("%")
          insertEventsdb(update(events_collection).set(mongoUpdate).where("accountTokenId" === (obj \ "accountTokenId") & "path" === (obj \ "path") & "timestamp" === (obj \ "timestamp") & "_id" === (obj \ "_id"))) map { _ =>
            print(".")
            //healthMonitor.sample("aggregation.complexity") { (complexity | 0L).toDouble }
            complexity
          }
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
