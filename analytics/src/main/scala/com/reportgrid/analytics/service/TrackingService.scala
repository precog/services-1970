package com.reportgrid.analytics
package service

import blueeyes._
import blueeyes.concurrent.Future
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.HttpHeaders._
import blueeyes.core.service._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.persistence.cache._
import blueeyes.util.Clock

import AnalyticsService._
import external.Jessup
import com.reportgrid.api.ReportGridTrackingClient
import com.reportgrid.api.Trackable
import rosetta.json.blueeyes._

import java.util.concurrent.TimeUnit
import scalaz.Scalaz._
import scalaz.Validation
import scalaz.Success
import scalaz.Failure
import scalaz.Semigroup
import scalaz.NonEmptyList

import com.weiglewilczek.slf4s.Logging

trait StorageReporting {
  def tokenId: String
  def stored(path: Path, count: Int)
  def stored(path: Path, count: Int, complexity: Long)
}

class NullStorageReporting(val tokenId: String) extends StorageReporting {
  def stored(path: Path, count: Int) = Unit
  def stored(path: Path, count: Int, complexity: Long) = Unit
}

class ReportGridStorageReporting(val tokenId: String, client: ReportGridTrackingClient[JValue]) extends StorageReporting {
  def expirationPolicy = ExpirationPolicy(
    timeToIdle = Some(30), 
    timeToLive = Some(120),
    timeUnit = TimeUnit.SECONDS
  )

  val stage = Stage[Path, StorageMetrics](expirationPolicy, 0) { 
    case (path, StorageMetrics(count, complexity)) =>
      client.track(
        Trackable(
          path = path.toString,
          name = "stored",
          properties = JObject(JField("#timestamp", "auto") :: JField("count", count) :: JField("complexity", complexity) :: Nil),
          rollup = true
        )
      )
  }

  def stored(path: Path, count: Int) = {
    stage.put(path, StorageMetrics(count, None))
  }
  
  def stored(path: Path, count: Int, complexity: Long) = {
    stage.put(path, StorageMetrics(count, Some(complexity)))
  }
}

/*
class LocalStorageReporting(val token Token, aggregationEngine: AggregationEngine, clock: Clock) extends StorageReporting {
  def expirationPolicy = ExpirationPolicy(
    timeToIdle = Some(30), 
    timeToLive = Some(120),
    timeUnit = TimeUnit.SECONDS
  )

  val stage = Stage[Path, StorageMetrics](expirationPolicy, 0) { 
    case (path, StorageMetrics(count, complexity)) => 
      val event = JObject(JField("~count", count) :: JField("~complexity", complexity) :: Nil)
      val tags = List(Tag("timestamp", TimeReference(AggregationEngine.timeSeriesEncoding, clock.instant())))

      path.rollups(path.length - 1) map { storagePath =>
        aggregationEngine.aggregate(
          token = token,
          path = storagePath,
          eventName = "stored",
          eventBody = event,
          allTags = tags,
          count = 1
        )
      }
  }

  def stored(path: Path, count: Int, complexity: Long) = stage.put(path, StorageMetrics(count, complexity))
}
*/

case class StorageMetrics(count: Int, complexity: Option[Long])
object StorageMetrics {
  implicit val Semigroup: Semigroup[StorageMetrics] = new Semigroup[StorageMetrics] {
    override def append(s1: StorageMetrics, s2: => StorageMetrics) = {
      val count = s1.count + s2.count
      val complexity = (s1.complexity, s2.complexity) match {
        case (None, None)         => None
        case (Some(c), None)      => Some(c)
        case (None, Some(c))      => Some(c)
        case (Some(c1), Some(c2)) => Some(c1 + c2)
      }
      StorageMetrics(count, complexity)
    }
  }
}

class TrackingService(aggregationEngine: AggregationEngine, storageReporting: StorageReporting, timeSeriesEncoding: TimeSeriesEncoding, clock: Clock, jessup: Jessup, autoTimestamp: Boolean)
extends CustomHttpService[Future[JValue], (Token, Path) => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    val tagExtractors = Tag.timeTagExtractor(timeSeriesEncoding, clock.instant(), autoTimestamp) ::
                        Tag.locationTagExtractor(jessup(request.remoteHost.orElse(request.headers.header(`X-Forwarded-For`).flatMap(_.ips.headOption.map(_.ip))))) :: Nil

    val count:  Int = request.parameters.get('count).flatMap(_.parseInt.toOption).getOrElse(1)

    Success(
      (token: Token, path: Path) => {
        val rollup: Int = request.parameters.get('rollup) flatMap { r => 
          r.parseBoolean match {
            case Success(true)  => Some(token.limits.rollup)
            case Success(false) => Some(0)
            case _ => r.parseInt.toOption.map(_.min(token.limits.rollup))
          }
        } getOrElse {
          0
        } 

        request.content.map { 
          _.flatMap {
            case obj @ JObject(fields) => 
              logger.debug(count + "|" + token.tokenId + "|" + path.path + "|" + compact(render(obj)))

              Future(
                fields flatMap { 
                  case JField(eventName, jvalue) => { 
                    def appendError = (errors: NonEmptyList[String]) => {
                      ("Errors occurred parsing the \"tag\" properties of the \"" + eventName + "\" event: " + compact(render(jvalue))) <:: errors
                    }

                    // compensate for bare jvalues as events
                    val event: JObject = jvalue match {
                      case obj: JObject => obj
                      case v => JObject(JField("value", v) :: Nil)
                    }
              
                    val offset = clock.now().minusDays(1).toInstant
                    val reprocess = (event \ "#timestamp").validated[String].flatMap(_.parseLong).exists(_ <= offset.getMillis)

                    val (tagResults, remainder) = Tag.extractTags(tagExtractors, event)

                    val trackableEvent = token.tokenId != storageReporting.tokenId
                    val billingPath = accountPath(path)

                    val storeEventFuture = (if (trackableEvent) {
                      aggregationEngine.store(token, path, eventName, jvalue, tagResults, count, rollup, reprocess).map(_ => ())
                    } else {
                      Future.sync(())
                    }).map(_ => 0L.success[NonEmptyList[String]])

                    storeEventFuture :: (if(reprocess) {
                      if (trackableEvent) storageReporting.stored(billingPath, path.rollups(rollup min path.length - 1).size)
                      List(Future.sync(0L.success[NonEmptyList[String]])) //skip immediate aggregation of historical data
                    } else {
                      // only roll up to the client root, and not beyond (hence path.length - 1)
                      path.rollups(rollup min (path.length - 1)) map { storagePath =>
                        aggregationEngine.aggregate(token, storagePath, eventName, tagResults, remainder, count) map { v => 
                          (appendError <-: v) 
                        } deliverTo { 
                          case Success(complexity) => 
                            // as a side effect, mark that an event was recorded in the storage reporting system
                            if (trackableEvent) storageReporting.stored(billingPath, 1, complexity)

                          case _ => 
                        }
                      }
                    })
                  } 
                }: _*
              ) map { results =>
                results.reduceOption(_ >>*<< _) match {
                  case Some(Success(complexity)) => 
                    logger.debug("Total complexity: " + complexity)
                    HttpResponse[JValue](OK)

                  case Some(Failure(errors)) => 
                    logger.debug("Encountered tag parsing errors: " + errors.list.mkString("; "))
                    HttpResponse[JValue](HttpStatus(BadRequest, "Errors occurred parsing tag properies of one or more of your events."), 
                                         content = Some(errors.list.mkString("", ";\n", "\n") + "original: " + pretty(render(obj))))

                  case None => 
                    logger.debug("Did not find any fields in content object: " + pretty(render(obj)))
                    HttpResponse[JValue](HttpStatus(BadRequest, "No trackable events were found in the content body."), 
                                         content = Some("original: " + pretty(render(obj))))
                }
              }

            case err => 
              Future.sync(HttpResponse[JValue](HttpStatus(BadRequest, "Body content not a JSON object."), 
                                               content = Some("Expected a JSON object but got " + pretty(render(err)))))
          }
        } getOrElse {
          Future.sync(HttpResponse[JValue](BadRequest, content = Some("Event content was not specified.")))
        }
      }
    )
  }

  private def accountPath(path: Path): Path = path.parent match {
    case Some(parent) if parent.equals(Path.Root) => path
    case Some(parent)                             => accountPath(parent)
    case None                                     => sys.error("Traversal to parent of root path should never occur.")
  }

  val metadata = Some(DescriptionMetadata(
    if (autoTimestamp) {
      """
        This service can be used to store a temporal event. If no timestamp tag is specified, then
        the service will be timestamped in UTC with the time on the ReportGrid servers.
      """
    } else {
      """
        This service can be used to store an data point with or without an associated timestamp. 
        Timestamps are not added by default.
      """
    }
  ))
}
