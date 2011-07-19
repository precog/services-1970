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

import org.apache.cassandra.thrift.ConsistencyLevel

import org.joda.time.Instant
import org.scale7.cassandra.pelops._
import org.scale7.cassandra.pelops.pool._

case class YggdrasilState(poolName: String, columnFamily: String, clock: Clock)

trait YggdrasilService extends BlueEyesServiceBuilder with BijectionsChunkJson with BijectionsChunkString {
  val yggdrasil = service("yggdrasil", "0.01") { context =>
    startup { 
      import context.config
      val poolName = "yggdrasilPool"
      val keyspace = "yggdrasil"
      val colFamily = "events"
      val cassandraConfig = config.configMap("cassandra")
      val cluster = new Cluster(cassandraConfig.getList("hosts").mkString(","), cassandraConfig.getInt("port", 9160))
      Pelops.addPool(poolName, cluster, keyspace)

      Future.sync(YggdrasilState(poolName, colFamily, ClockSystem.realtimeClock))
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

                  //create a sortable, globally unique key - the UUID is just a salt
                  val rowKey = tokenId + ":" + timestamp.getMillis + ":" + prefixPath + "#" + UUID.randomUUID.toString

                  events.fields.foreach {
                    case JField(eventName, event) => 
                      val mutator = Pelops.createMutator(poolName)
                      mutator.writeSubColumns(
                        columnFamily, rowKey, eventName, 
                        mutator.newColumnList(
                          events.flattenWithPath.map {
                            case (jpath, jvalue) => mutator.newColumn(jpath.path, renderNormalized(jvalue))
                          }: _*
                        )
                      );

                      mutator.execute(ConsistencyLevel.ONE);
                  }
                }

                HttpResponse.empty[JValue]
              }
            }
          }
        }
      } ~ 
      orFail { request: HttpRequest[ByteChunk] =>
        println("Could not handle request " + request)
        (HttpStatusCodes.NotFound, "No handler matched the request " + request)
      }
    } ->
    shutdown { state => Future.sync(()) }
  }
}

object Yggdrasil extends BlueEyesServer with YggdrasilService 

// vim: set ts=4 sw=4 et:
