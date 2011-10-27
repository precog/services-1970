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
import scalaz.Success
import scalaz.Scalaz._

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

                if (!reprocess) {
                  val (tagResults, remainder) = Tag.extractTags(tagExtractors, event)
                  getTags(tagResults).flatMap(aggregationEngine.aggregate(token, path, eventName, _, remainder, count))
                } else {
                  Future.sync(0L)
                }
              }: _*
            ) map {
              v => HttpResponse[JValue](content = Some(v.sum))
            }

          case err => 
            Future.sync(HttpResponse[JValue](BadRequest, content = Some("Expected a JSON object but got " + pretty(render(err)))))
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
