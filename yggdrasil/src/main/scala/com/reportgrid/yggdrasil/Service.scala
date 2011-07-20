package com.reportgrid.yggdrasil

import blueeyes.{BlueEyesServiceBuilder, BlueEyesServer}
import blueeyes.core.data.{BijectionsChunkJson, BijectionsChunkString, ByteChunk}
import blueeyes.core.http._
import blueeyes.concurrent.Future
import blueeyes.json.JsonAST._
import blueeyes.json.Printer._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.JodaSerializationImplicits._
import blueeyes.util.{Clock, ClockSystem}

import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.Instant

case class YggdrasilState(configuration: Configuration, clock: Clock)

trait YggdrasilService extends BlueEyesServiceBuilder with BijectionsChunkJson with BijectionsChunkString {
  val hTable = "yggdrasil"
  val columnFamily = Bytes.toBytes("events")
  val countColumn = Bytes.toBytes("count")

  val yggdrasil = service("yggdrasil", "0.01") { context =>
    startup { 
      import context.config
      val hbaseConfig = HBaseConfiguration.create

      Future.sync(YggdrasilState(hbaseConfig, ClockSystem.realtimeClock))
    } -> 
    request { (state: YggdrasilState) => 
      import state._
      jvalue {
        path("""/vfs/(?:(?<prefixPath>(?:[^\n.](?:[^\n/]|/[^\n\.])+)/?)?)""") { 
          $ {
            post { request: HttpRequest[JValue] => 
              Future.async {
                val tokenId = request.parameters.get('tokenId)
                val prefixPath = request.parameters.get('prefixPath).flatMap(Option(_))

                request.content.foreach { content =>
                  val timestamp: Instant = (content \ "timestamp") match {
                    case JNothing => clock.instant()
                    case jvalue   => jvalue.deserialize[Instant]
                  }

                  val events: JObject = (content \ "events") --> classOf[JObject]

                  val count: Int = (content \ "count") match {
                    case JNothing => 1
                    case jvalue   => jvalue.deserialize[Int]
                  }

                  val table = new HTable(configuration, hTable)
                  try {
                    events.fields.foreach {
                      case JField(eventName, event) => 
                        //create a sortable, globally unique key - the UUID is just a salt
                        val rowKey = tokenId + ":" + prefixPath + "/" + eventName + ":" + timestamp.getMillis + "#" + UUID.randomUUID.toString
                        val put  = new Put(Bytes.toBytes(rowKey))
                        put.add(columnFamily, countColumn, Bytes.toBytes(count))
                        event.flattenWithPath.foreach {
                          case (jpath, jvalue) => put.add(
                            columnFamily, 
                            Bytes.toBytes(jpath.path), 
                            Bytes.toBytes(renderNormalized(jvalue))
                          )
                        }
                        table.put(put)
                    }
                  } finally {
                    table.close
                  }
                }

                HttpResponse.empty[JValue]
              }
            }
          }
        }
      } ~ 
      orFail { request: HttpRequest[ByteChunk] =>
        (HttpStatusCodes.NotFound, "No handler matched the request " + request)
      }
    } ->
    shutdown { state => Future.sync(()) }
  }
}

object Yggdrasil extends BlueEyesServer with YggdrasilService 

// vim: set ts=4 sw=4 et:
