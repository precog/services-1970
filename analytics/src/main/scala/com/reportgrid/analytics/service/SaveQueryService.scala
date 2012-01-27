package com.reportgrid.analytics
package service

import java.util.UUID
import javax.xml.bind.DatatypeConverter

import org.joda.time.{Duration,Instant}

import blueeyes.concurrent.Future
import blueeyes.core.data.{ByteChunk,MemoryChunk}
import blueeyes.core.http.{HttpException, HttpMethod, HttpMethods, HttpRequest, HttpResponse, HttpStatusCodes, MimeType, MimeTypes, URI}
import HttpStatusCodes._
import blueeyes.core.service.{DelegatingService, DispatchError, HttpService, JsonpService, NotServed}
import blueeyes.persistence.mongo.{Database,MongoCollection,MongoImplicits}
import MongoImplicits._

import blueeyes.json.{JPath,Printer}
import blueeyes.json.JsonAST._
import Printer._
import blueeyes.json.xschema.{Decomposer,DefaultSerialization,Extractor,JodaSerializationImplicits}
import DefaultSerialization._
import JodaSerializationImplicits._

import com.weiglewilczek.slf4s.Logging

import scalaz.Scalaz._
import scalaz.Validation

/**
 * This class can optionally save the request query for later replay/caching
 *
 * To save, pass a "save" parameter in the request. A replay ID will be returned.
 * To replay, pass a "replay" parameter in the request with a replay ID.
 *
 * TODO: Fix to handle ByteChunk chain
 */
class SaveQueryService(database: Database)(val delegate: HttpService[ByteChunk, Future[HttpResponse[ByteChunk]]]) extends DelegatingService[ByteChunk, Future[HttpResponse[ByteChunk]], ByteChunk, Future[HttpResponse[ByteChunk]]] with Logging {
  val saved_query_collection: MongoCollection = "saved_queries"
  val cached_results_collection: MongoCollection = "cached_results"

  // TODO ensure indices on collections

  def service = (req: HttpRequest[ByteChunk]) => {
    (req.parameters.get('replay), req.parameters.get('save)) match {
      case (Some(replayId),_) => processReplay(replayId, req)
      case (_, Some(_))       => processSave(req)
      case _                  => {
        // No replay services requested, pass-through
        delegate.service(req)
      }
    }
  }

  val metadata = None

  def processReplay(replayId: String, req: HttpRequest[ByteChunk]): Validation[NotServed, Future[HttpResponse[ByteChunk]]] = success {
    // Locate the saved query data so that we can determine which paramters may/will be replaced
    val savedFilter = JPath(".replayId") === replayId.serialize
    logger.debug("Locating saved query for " + replayId)
    logger.trace("with filter " + savedFilter.filter)

    database(selectOne().from(saved_query_collection).where(savedFilter)).flatMap { _.map(_.deserialize[SavedQuery]) match {
      case Some(saved) => {
        logger.trace("Request params = " + req.parameters)
        logger.trace("Allowed overrides = " + saved.allowedParamReplacement)

        // Compute the overridden params, if any
        val replacedParams = req.parameters.map { case (k,v) => (k.name, v) }.filterKeys(k => k != "path" && saved.allowedParamReplacement.contains(k))
        logger.trace("replaced parameters on saved query %s are %s".format(replayId, replacedParams))

        val replacedParamObj = Printer.normalize(replacedParams.serialize)

        // Check for a path override
        val requestURI = if (saved.allowedParamReplacement.contains("path") && req.parameters.contains('path)) {
          logger.debug("Using overridden path: " + req.parameters.get('path))
          saved.uri.copy(path = req.parameters.get('path))
        } else {
          saved.uri
        }

        val cacheFilter = (JPath(".replayId") === replayId.serialize) & (JPath(".replacedParams") === replacedParamObj) &
                          (JPath(".path") === requestURI.path.getOrElse("").serialize) & (JPath(".expires") > System.currentTimeMillis.serialize)

        logger.trace("Searching for cached query data with filter " + compact(render(cacheFilter.filter)))
        
        // Locate any existing cached result
        database(selectOne().from(cached_results_collection).where(cacheFilter)).flatMap(_.map(_.deserialize[CachedQuery]) match {
          case Some(cached) => {
            logger.debug("Returning cached response for " + replayId)
            Future.sync(HttpResponse[ByteChunk](content = Some(new MemoryChunk(cached.result))))
          }
          case None         => {
            // Compute the full set of original and replaced params
            val newParams = (saved.params ++ replacedParams).map{ case (k,v) => (Symbol(k), v) }
            logger.trace("Parameters for saved query %s are %s".format(replayId, newParams))

            // Need to replay the request and cache the result
            val newReq = HttpRequest[ByteChunk](saved.method, requestURI, newParams)
            
            logger.trace("Running query with request: " + newReq)

            val result = delegate.service(newReq)

            result.foreach(_.deliverTo {
              data => {
                try {
                  val cacheEntry = CachedQuery(replayId, replacedParams, requestURI.path.getOrElse(""), (new Instant).plus(saved.maxCache), data.content.map(_.data).getOrElse(Array())).serialize.asInstanceOf[JObject]
                  database(upsert(cached_results_collection).set(cacheEntry).where(cacheFilter)).deliverTo {
                    _ => logger.debug("Cached query result for " + replayId)
                  }.trap {
                    errors => logger.error("Error(s) on query cache insert: " + errors)
                  }
                } catch {
                  case e => logger.error("Error on query cache insert: " + e)
                }
              }
            })

            result.fold(_ => Future.sync(HttpResponse(InternalServerError)), good => good)
          }
        })
      }
      case _           => Future.sync(HttpResponse(NotFound, content = Some(new MemoryChunk(("Could not locate replay ID " + replayId).getBytes("UTF-8"))))) // TODO : there has to be a simpler way
    }}
  }

  /**
   * Saves the current request for later replay
   *
   * TODO: validate the request
   * 
   * schema:
   * { replayId : <UUID>,
   *   created  : <timestamp>,
   *   validTil : <timestamp>,
   *   maxCache : <duration in ms>,
   *   uri      : <string uri>,
   *   params   : [{ name: <param name>, value: <param value> }, ...],
   *   body     : <binary string>,
   *   allowedOverrides: [<param name>,...] }
   */ 
  def processSave(req: HttpRequest[ByteChunk]): Validation[NotServed, Future[HttpResponse[ByteChunk]]] = {
    val saveRecord = SavedQuery(req)
    val saveObj = saveRecord.serialize.asInstanceOf[JObject]

    // TODO: Should deal with a UUID collision, however unlikely
    success(database(upsert(saved_query_collection).set(saveObj).where(JPath(".replayId") === saveRecord.replayId.toString)).map {
      _ => HttpResponse(headers = List("Content-Type" -> "text/plain"), content = Some(new MemoryChunk(saveRecord.replayId.toString.getBytes("UTF-8"))))
    })
  }
}

case class CachedQuery(replayId: String,
                       replacedParams: Map[String,String],
                       path: String,
                       expires: Instant,
                       result: Array[Byte])

trait CachedQuerySerialization {
  implicit val CachedQueryDecomposer: Decomposer[CachedQuery] = new Decomposer[CachedQuery] {
    def decompose(c: CachedQuery): JValue =
      JObject(List(JField("replayId", c.replayId.serialize),
                   JField("replacedParams", Printer.normalize(c.replacedParams.serialize)),
                   JField("path", c.path.serialize),
                   JField("expires", c.expires.serialize),
                   JField("result", DatatypeConverter.printBase64Binary(c.result).serialize)))
  }

  implicit val CachedQueryExtractor: Extractor[CachedQuery] = new Extractor[CachedQuery] {
    def extract(jv: JValue): CachedQuery =
      CachedQuery((jv \ "replayId").deserialize[String],
                  (jv \ "replacedParams").deserialize[Map[String,String]],
                  (jv \ "path").deserialize[String],
                  (jv \ "expires").deserialize[Instant],
                  DatatypeConverter.parseBase64Binary((jv \ "result").deserialize[String]))
  }
}

object CachedQuery extends CachedQuerySerialization                     

case class SavedQuery(replayId: String,
                      created: Instant,
                      validTil: Instant,
                      maxCache: Duration,
                      method: HttpMethod,
                      uri: URI,
                      params: Map[String,String],
                      allowedParamReplacement: List[String],
                      body: Array[Byte])

trait SavedQuerySerialization {
  implicit val SavedQueryDecomposer: Decomposer[SavedQuery] = new Decomposer[SavedQuery] {
    def decompose(s: SavedQuery): JValue =
      JObject(List(JField("replayId", s.replayId.serialize),
                   JField("created", s.created.serialize),
                   JField("validTil", s.validTil.serialize),
                   JField("maxCache", s.maxCache.serialize),
                   JField("method", s.method.toString.serialize),
                   JField("uri", s.uri.toString.serialize),
                   JField("params", s.params.serialize),
                   JField("allowedParamReplacement", s.allowedParamReplacement.serialize),
                   JField("body", DatatypeConverter.printBase64Binary(s.body).serialize)))
  }

  implicit val SavedQueryExtractor: Extractor[SavedQuery] = new Extractor[SavedQuery] {
    def extract(jv: JValue): SavedQuery =
      SavedQuery((jv \ "replayId").deserialize[String],
                 (jv \ "created").deserialize[Instant],
                 (jv \ "validTil").deserialize[Instant],
                 (jv \ "maxCache").deserialize[Duration],
                 HttpMethods.parseHttpMethods((jv \ "method").deserialize[String]).head,
                 URI((jv \ "uri").deserialize[String]),
                 (jv \ "params").deserialize[Map[String,String]],
                 (jv \ "allowedParamReplacement").deserialize[List[String]],
                 DatatypeConverter.parseBase64Binary((jv \ "body").deserialize[String]))
  }
}

object SavedQuery extends SavedQuerySerialization {
  val reservedParams = List('save, 'replay, 'allowedParams)

  def apply(req: HttpRequest[ByteChunk]) : SavedQuery = {
    val id = UUID.randomUUID.toString.toUpperCase

    val params = req.parameters.filterKeys{ k => ! reservedParams.contains(k) }.map{ case (k,v) => (k.name,v) }

    // We always allow an override of the callback, since it's unlikely someone will maintain the same callback ID
    val allowedOverrides : List[String] = "callback" :: req.parameters.get('allowedParams).map(_.split(',').toList).getOrElse(Nil)

    SavedQuery(id,
               new Instant,
               new Instant(Long.MaxValue),
               Duration.standardDays(365 * 100), // 100 years really ought to be close enough to forever ;)
               req.method,
               req.uri.copy(query = None), // Discard the query params since we've already saved the ones we want
               params,
               allowedOverrides, 
               req.content.map(_.data).getOrElse(Array()))
  }
}
    
    
