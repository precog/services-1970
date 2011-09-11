package com.reportgrid
package blueeyes 

import _root_.blueeyes._
import _root_.blueeyes.util._
import _root_.blueeyes.core.http._
import _root_.blueeyes.core.http.HttpHeaders._
import _root_.blueeyes.core.service._
import _root_.blueeyes.concurrent.Future
import _root_.blueeyes.json.JsonAST._
import _root_.blueeyes.json.xschema.DefaultSerialization._
import com.reportgrid.analytics.Token
import com.reportgrid.api._
import rosetta.json.blueeyes._
import org.joda.time.Duration
import org.joda.time.Instant

import scalaz.Scalaz._

trait ReportGridInstrumentation {
  val ReportGridUserAgent = "ReportGrid Introspection Agent / 1.0"

  def bucketCounts(i: Long) = if (i / 25000 > 0) (i / 10000) * 10000
                              else if (i / 2500 > 0) (i / 1000) * 1000
                              else if (i / 250 > 0) (i / 100) * 100
                              else if (i / 25 > 0) (i / 10) * 10
                              else i

  def auditor[T, S](client: ReportGridTrackingClient[JValue], clock: Clock, tokenOf: HttpRequest[T] => Future[Token]) = {
    (function: String) => record(
      (req: HttpRequest[T], resp: HttpResponse[S], start: Instant, end: Instant) => tokenOf(req) map { 
        token => Trackable(
          path = "/" + token.accountTokenId + "/latencies",
          name = "request",
          properties = JObject(List(
            JField("latency", bucketCounts(new Duration(start, end).getMillis)),
            JField("method", req.method.toString),
            JField("function", function)
          )),
          rollup = true,
          tags = Set(TimeTag[JValue](start.toDate))
        )
      },
      client, 
      clock
    )
  }

  def record[T, S](f: (HttpRequest[T], HttpResponse[S], Instant, Instant) => Future[Trackable[JValue]], client: ReportGridTrackingClient[JValue], clock: Clock)  = (next: HttpRequestHandler2[T, S]) => new HttpRequestHandler2[T, S] {
    override def isDefinedAt(req: HttpRequest[T]) = next.isDefinedAt(req)
  
    override def apply(req: HttpRequest[T]) = {
      // if the request comes from the introspection agent, pass it through and ignore it.
      if (req.headers.header[`User-Agent`].exists(_.value == ReportGridUserAgent)) next(req)
      else clock.instant() |> { start => 
        next(req) deliverTo { resp => 
          clock.instant() |> { end => 
            f(req, resp, start, end).flatMap { (trackable: Trackable[JValue]) => 
              Future.async(client.track(trackable.copy(headers = trackable.headers ++ Map(`User-Agent`.name -> ReportGridUserAgent))))
            }.toUnit
          }
        }
      }
    }
  }
}



// vim: set ts=4 sw=4 et:
