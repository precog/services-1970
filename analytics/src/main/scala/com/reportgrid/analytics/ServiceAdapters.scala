package com.reportgrid.analytics
package external

import blueeyes.concurrent.Future
import blueeyes.core.data.{ByteChunk, Bijection, BijectionsChunkJson}
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.json.JsonAST._
import rosetta.json.blueeyes._

import java.net.InetAddress
import java.util.Date
import scalaz.Scalaz._

import com.reportgrid.instrumentation.blueeyes.ReportGridInstrumentation
import com.reportgrid.api.{NoRollup, ReportGridTrackingClient, RollupLimit, Tag}
import com.reportgrid.api.blueeyes._
import com.weiglewilczek.slf4s.Logging

object NoopTrackingClient extends ReportGridTrackingClient[JValue](JsonBlueEyes) with Logging {
  override def track(path: com.reportgrid.api.Path, name: String, properties: JValue = JsonBlueEyes.EmptyObject, rollup: RollupLimit = NoRollup, tags: Set[Tag[JValue]] = Set.empty[Tag[JValue]], count: Option[Int] = None, headers: Map[String, String] = Map.empty): Unit = {
    logger.trace("Tracked " + path + "; " + name + " - " + properties)
  }
}

trait Jessup {
  def apply(host: Option[InetAddress]): Future[Option[Hierarchy]]
}

object Jessup {
  def Noop = new Jessup {
    override def apply(host: Option[InetAddress]) = Future.sync(None)
  }
}

class JessupServiceProxy(host: String, port: Option[Int], path: String) extends Jessup with BijectionsChunkJson with Logging {
  val client = new HttpClientXLightWeb().translate[JValue].host(host).port(port getOrElse 80).path(path)

  override def apply(host: Option[InetAddress]): Future[Option[Hierarchy]] = {
    logger.debug("Querying Jessup for " + host)
    host map { host =>
      client.get[JValue](host.getHostAddress) map { response =>
        response.content flatMap { jvalue =>
          val locations = (jvalue \ "country-name") match {
            case JString(countryName) => {
              Hierarchy.AnonLocation(Path("/%s".format(countryName))) :: {
                (jvalue \ "region") match {
                  case JString(region) => {
                    Hierarchy.AnonLocation(Path("/%s/%s".format(countryName, region))) :: {
                      (jvalue \ "city") match {
                        case JString(city) => {
                          Hierarchy.AnonLocation(Path("/%s/%s/%s".format(countryName, region, city))) :: {
                            (jvalue \ "postal-code") match {
                              case JString(postalCode) => Hierarchy.AnonLocation(Path("/%s/%s/%s/%s".format(countryName, region, city, postalCode))) :: Nil
                              case _ => Nil
                            }
                          }
                        }
                        case _ => Nil
                      }
                    }
                  }
                  case _ => Nil
                }
              }
            }
            case _ => Nil
          }
          val back = Hierarchy of (locations)
            
          back.toOption
        }
      }
    } getOrElse {
      Future.sync(None)
    }
  }
}

// vim: set ts=4 sw=4 et:
