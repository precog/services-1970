package com.reportgrid.analytics
package external

import com.reportgrid.blueeyes.ReportGridInstrumentation
import blueeyes.concurrent.Future
import blueeyes.core.data.{ByteChunk, Bijection, BijectionsChunkJson}
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.json.JsonAST._
import com.reportgrid.api.ReportGridTrackingClient
import com.reportgrid.api.blueeyes._
import rosetta.json.blueeyes._

import java.net.InetAddress
import java.util.Date
import scalaz.Scalaz._

object NoopTrackingClient extends ReportGridTrackingClient[JValue](JsonBlueEyes) {
  override def track(path: com.reportgrid.api.Path, name: String, properties: JValue = JsonBlueEyes.EmptyObject, rollup: Boolean = false, timestamp: Option[Date] = None, count: Option[Int] = None, headers: Map[String, String] = Map.empty): Unit = {
    //println("Tracked " + path + "; " + name + " - " + properties)
  }
}

trait Yggdrasil[T] {
  def apply(handler: HttpRequestHandler[T]): HttpRequestHandler[T]
}

object Yggdrasil {
  def Noop[T] = new Yggdrasil[T] {
    override def apply(handler: HttpRequestHandler[T]) = handler
  }
}

class YggdrasilServiceProxy[T:({type B[X] = Bijection[X,ByteChunk]})#B](host: String, port: Option[Int], path: String)
extends Yggdrasil[T] with ReportGridInstrumentation with HttpRequestHandlerCombinators {
  val client: HttpClient[T] = new HttpClientXLightWeb().translate[T]

  def yggdrasilRewrite(req: HttpRequest[T]): Option[HttpRequest[T]] = {
    import HttpHeaders._
    (!req.headers.header[`User-Agent`].exists(_.value == ReportGridUserAgent)).option {
      val prefixPath = req.parameters.get('prefixPath).getOrElse("")
      req.copy(
        uri = req.uri.copy(
          host = Some(host), 
          port = port, 
          path = Some(path + "/vfs/" + prefixPath)
        ),
        parameters = req.parameters - 'prefixPath
      ) 
    }
  }

  override def apply(handler: HttpRequestHandler[T]) = forwarding[T, T](yggdrasilRewrite, client)(handler)
}

trait Jessup {
  def apply(host: InetAddress): Future[Option[Hierarchy]]
}

object Jessup {
  def Noop = new Jessup {
    override def apply(host: InetAddress) = Future.sync(None)
  }
}

class JessupServiceProxy(host: String, port: Option[Int], path: String) extends Jessup with BijectionsChunkJson {
  val client = new HttpClientXLightWeb().translate[JValue].host(host).port(port getOrElse 80).path(path)

  override def apply(host: InetAddress): Future[Option[Hierarchy]] = {
    client.get[JValue]("/" + host.getHostAddress) map { response =>
      response.content flatMap { jvalue =>
        val JString(countryName) = jvalue \ "country-name"
        val JString(region) = jvalue \ "region"
        val JString(city) = jvalue \ "city"
        val JString(postalCode) = jvalue \ "postal-code"
        
        val back = Hierarchy of (
          Hierarchy.AnonLocation(Path("/%s".format(countryName))) ::
          Hierarchy.AnonLocation(Path("/%s/%s".format(countryName, region))) ::
          Hierarchy.AnonLocation(Path("/%s/%s/%s".format(countryName, region, city))) ::
          Hierarchy.AnonLocation(Path("/%s/%s/%s/%s".format(countryName, region, city, postalCode))) :: Nil)
          
        back.toOption
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
