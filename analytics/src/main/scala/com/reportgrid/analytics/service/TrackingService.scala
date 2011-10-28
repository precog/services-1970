package com.reportgrid.analytics
package service

import blueeyes.concurrent.Future
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.util.Clock

import AnalyticsService._
import external.Jessup
import scalaz.Scalaz._
import scalaz.Success
import scalaz.Failure
import scalaz.NonEmptyList

class TrackingService(aggregationEngine: AggregationEngine, timeSeriesEncoding: TimeSeriesEncoding, clock: Clock, jessup: Jessup, autoTimestamp: Boolean)
extends CustomHttpService[Future[JValue], (Token, Path) => Future[HttpResponse[JValue]]] {
  val service = (request: HttpRequest[Future[JValue]]) => {
    val tagExtractors = Tag.timeTagExtractor(timeSeriesEncoding, clock.instant(), autoTimestamp) ::
                        Tag.locationTagExtractor(jessup(request.remoteHost))             :: Nil

    val count: Int = request.parameters.get('count).map(_.toInt).getOrElse(1)

    Success(
      (token: Token, path: Path) => request.content.map { 
        _.flatMap {
          case obj @ JObject(fields) => 
            aggregationEngine.logger.debug(count + "|" + token.tokenId + "|" + path.path + "|" + compact(render(obj)))

            Future(
              fields.map { case JField(eventName, jvalue) => 
                // compensate for bare jvalues as events
                val event: JObject = jvalue match {
                  case obj: JObject => obj
                  case v => JObject(JField("value", v) :: Nil)
                }
          
                val offset = clock.now().minusDays(1).toInstant
                val reprocess = (event \ "#timestamp").validated[String].flatMap(_.parseLong).exists(_ <= offset.getMillis)
                aggregationEngine.store(token, path, eventName, jvalue, count, reprocess)

                val ingestResult = if (!reprocess) {
                  val (tagResults, remainder) = Tag.extractTags(tagExtractors, event)
                  aggregationEngine.aggregate(token, path, eventName, tagResults, remainder, count)
                } else {
                  Future.sync(0L.success[NonEmptyList[String]])
                } 

                ingestResult map {
                  _.fail.map((errors: NonEmptyList[String]) => ("Errors occurred parsing the \"tag\" properties of the \"" + eventName + "\" event: " + compact(render(jvalue))) <:: errors).validation
                }
              }: _*
            ) map {
              _.reduceOption(_ >>*<< _) match {
                case Some(Success(complexity)) => 
                  aggregationEngine.logger.debug("total complexity: " + complexity)
                  HttpResponse[JValue](OK)

                case Some(Failure(errors)) => 
                  aggregationEngine.logger.debug("Encountered tag parsing errors: " + errors.list.mkString("; "))
                  HttpResponse[JValue](HttpStatus(BadRequest, "Errors occurred parsing tag properies of one or more of your events."), content = Some(errors.list.mkString("; ")))

                case None => 
                  //aggregationEngine.logger.debug("No trackable events in content body.")
                  HttpResponse[JValue](HttpStatus(BadRequest, "No trackable events were found in the content body."))
              }
            }

          case err => 
            Future.sync(HttpResponse[JValue](HttpStatus(BadRequest, "Body content not a JSON object."), content = Some("Expected a JSON object but got " + pretty(render(err)))))
        }
      } getOrElse {
        Future.sync(HttpResponse[JValue](BadRequest, content = Some("Event content was not specified.")))
      }
    )
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
