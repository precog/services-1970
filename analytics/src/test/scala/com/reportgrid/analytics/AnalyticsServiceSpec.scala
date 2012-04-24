package com.reportgrid
package analytics

import java.net.URLEncoder

import blueeyes._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.concurrent.Future
import blueeyes.concurrent.test._
import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.xschema.JodaSerializationImplicits._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.JPathImplicits._
import blueeyes.persistence.mongo.{Mongo, RealMongo, MockMongo, MongoCollection, Database}
import blueeyes.util.metrics.Duration._
import blueeyes.util.Clock
import MimeTypes._

import org.joda.time._
import net.lag.configgy.ConfigMap

import org.specs2.mutable.Specification
import org.specs2.matcher.Matcher
import org.specs2.matcher.MatchResult
import org.specs2.matcher.Expectable
import org.specs2.specification.{Outside, Scope}
import org.scalacheck.Gen._
import scalaz.Success
import scalaz.Scalaz._


import Periodicity._
import AggregationEngine.ResultSet
import persistence.MongoSupport._
import com.reportgrid.api._
import com.reportgrid.api.blueeyes.ReportGrid
import com.reportgrid.ct._
import com.reportgrid.ct.Mult._
import com.reportgrid.ct.Mult.MDouble._
import service._

import BijectionsChunkJson._
import BijectionsChunkString._
import BijectionsChunkFutureJson._

import rosetta.json.blueeyes._

case class PastClock(duration: Duration) extends Clock {
  def now() = new DateTime().minus(duration)
  def instant() = now().toInstant
  def nanoTime = sys.error("nanotime not available in the past")
}

trait TestTokens {
  val TestToken = Token(
    tokenId        = "C7A18C95-3619-415B-A89B-4CE47693E4CC",
    parentTokenId  = Some(Token.Root.tokenId),
    accountTokenId = "C7A18C95-3619-415B-A89B-4CE47693E4CC",
    path           = "unittest",
    permissions    = Permissions(true, true, true, true),
    expires        = Token.Never,
    limits         = Limits(order = 2, depth = 5, limit = 20, tags = 2, rollup = 2)
  )

 val TrackingToken = Token(
    tokenId        = "DB6DEF4F-678A-4F7D-9897-F920762887F1",
    parentTokenId  = Some(Token.Root.tokenId),
    accountTokenId = "DB6DEF4F-678A-4F7D-9897-F920762887F1",
    path           = "__usage_tracking__",
    permissions    = Permissions(true, true, true, true),
    expires        = Token.Never,
    limits         = Limits(order = 1, depth = 2, limit = 5, tags = 1, rollup = 2, lossless=false)
 )
}

trait TestAnalyticsService extends BlueEyesServiceSpecification with AnalyticsService with LocalMongo with TestTokens {

  val requestLoggingData = """
    requestLog {
      enabled = true
      fields = "time cs-method cs-uri sc-status cs-content"
    }
  """

  override val clock = Clock.System

  override val configuration = "services{analytics{v1{" + requestLoggingData + mongoConfigFileData + "}}}"

  override def mongoFactory(config: ConfigMap): Mongo = new RealMongo(config)
  //override def mongoFactory(config: ConfigMap): Mongo = new MockMongo()

  def auditClient(config: ConfigMap) = external.NoopTrackingClient
  def jessup(configMap: ConfigMap) = external.Jessup.Noop

  def tokenManager(database: Database, tokensCollection: MongoCollection, deletedTokensCollection: MongoCollection): TokenManager = {
    val mgr = new TokenManager(database, tokensCollection, deletedTokensCollection) 
    mgr.tokenCache.put(TestToken.tokenId, TestToken)
    mgr.tokenCache.put(TrackingToken.tokenId, TrackingToken)
    mgr
  }

  def storageReporting(config: ConfigMap) = {
    val testServer = Server("/")

    val testHttpClient = new HttpClient[String] {
      def request(method: String, url: String, content: Option[String], headers: Map[String, String] = Map.empty[String, String]): String = {
        val httpMethods = HttpMethods.parseHttpMethods(method)
        val httpMethod = httpMethods match {
          case m :: Nil => m
          case _        => sys.error("Only one http method expected")
        }
        val chunkContent = content.map(StringToChunk(_))
        service.apply(HttpRequest(httpMethod, url, Map(), headers, chunkContent)).map(_.content.map(ChunkToString).getOrElse("")).toAkka.get
      }
    }

    val clientConfig = new ReportGridConfig(
      TrackingToken.tokenId,
      testServer,
      testHttpClient
    )
    val testClient = new ReportGridClient(clientConfig)

    new ReportGridStorageReporting(TrackingToken.tokenId, testClient) 
  }


  lazy val jsonTestService = service.contentType[JValue](application/(MimeTypes.json)).
                                     query("tokenId", TestToken.tokenId)

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(20, toDuration(1000L).milliseconds)
  val shortFutureTimeouts = FutureTimeouts(5, toDuration(50L).milliseconds)
}

class AnalyticsServiceSpec extends TestAnalyticsService with ArbitraryEvent with FutureMatchers {
  override val genTimeClock = clock 

  object sampleData extends Outside[List[Event]] with Scope {
    val outside = containerOfN[List, Event](50, fullEventGen).sample.get ->- {
      _.foreach(event => jsonTestService.post[JValue]("/vfs/test")(event.message))
    }
  }

  "Analytics Service" should {
    "create child tokens without a trailing slash" in {
        val newToken = TestToken.issue(permissions = Permissions(read = true, write = true, share = false, explore = false))
        jsonTestService.post[JValue]("/tokens")(newToken.serialize) must whenDelivered {
          beLike {
            case HttpResponse(HttpStatus(status, _), _, Some(JString(tokenId)), _) => 
              (status must_== HttpStatusCodes.OK) and 
              (tokenId.length must_== TestToken.tokenId.length) and
              (jsonTestService.get[JValue]("/tokens") must whenDelivered[HttpResponse[JValue]]({
                beLike {
                  case HttpResponse(status, _, Some(JArray(tokenIds)), _) => 
                    (tokenIds must contain(JString(tokenId))) and 
                    (jsonTestService.get[JValue]("/tokens/" + tokenId) must whenDelivered[HttpResponse[JValue]]({
                      beLike[HttpResponse[JValue]] {
                        case HttpResponse(status, _, Some(jtoken), _) => 
                          jtoken.validated[Token] must beLike {
                            case Success(token) => 
                              (token.permissions.read must beTrue) and 
                              (token.permissions.share must beFalse) and
                              (token.tokenId must_== tokenId)
                          }
                      }
                    })(shortFutureTimeouts))
                }
              })(shortFutureTimeouts))
          }
        }
    }

    "create child tokens with a trailing slash" in {
      val newToken = TestToken.issue(permissions = Permissions(read = true, write = true, share = false, explore = false))
      jsonTestService.post[JValue]("/tokens/")(newToken.serialize) must whenDelivered {
        beLike {
          case HttpResponse(HttpStatus(status, _), _, Some(JString(tokenId)), _) => 
            (status must_== HttpStatusCodes.OK) and 
            (tokenId.length must_== TestToken.tokenId.length) and
            (jsonTestService.get[JValue]("/tokens/") must whenDelivered {
              beLike {
                case HttpResponse(status, _, Some(JArray(tokenIds)), _) => tokenIds must contain(JString(tokenId))
              }
            })
        }
      }
    }

    "mark removed tokens as deleted" in {
      val newToken = TestToken.issue(permissions = Permissions(read = true, write = true, share = false, explore = false))
      val insert = jsonTestService.post[JValue]("/tokens/")(newToken.serialize)
      
      insert flatMap {
        case HttpResponse(HttpStatus(HttpStatusCodes.OK, _), _, Some(JString(tokenId)), _) => 
          for {
            HttpResponse(HttpStatus(HttpStatusCodes.OK, _), _, _, _) <- jsonTestService.delete[JValue]("/tokens/" + tokenId) 
            HttpResponse(HttpStatus(HttpStatusCodes.OK, _), _, Some(JArray(tokenIds)), _) <- jsonTestService.get[JValue]("/tokens/")
          } yield {
            tokenIds.contains(JString(tokenId))
          }

        case other => sys.error("Token insert failed: " + other)
      } must whenDelivered {
        beFalse
      }
    }

    "return a sensible result when deleting a non-existent token" in {
      val newToken = TestToken.issue(permissions = Permissions(read = true, write = true, share = false, explore = false))

      jsonTestService.delete[JValue]("/tokens/" + newToken.tokenId) must whenDelivered {
        beLike {
          case HttpResponse(HttpStatus(code, message), _, result, _) => code must_== HttpStatusCodes.BadRequest
        }
      }
    }

    "retrieve values for a given property" in sampleData {
      sampleEvents => {
        val eventName = sampleEvents.head.eventName

        val expected : List[JValue] = sampleEvents.filter {
          ev => ev.eventName == eventName
        }.map(_.data.value.get("num")).distinct
        
        jsonTestService.get[JValue]("/vfs/test/." + eventName + ".num/values") must whenDelivered {
          beLike {
              case HttpResponse(HttpStatus(status, _), _, Some(result), _) => {
                val nonTagData = result.deserialize[List[JValue]].map {
                  case JObject(fields) => JObject(fields.filterNot(_.name.startsWith("#")))
                  case other => other
                }
                (status must_== HttpStatusCodes.OK) and
                (nonTagData must containAllOf(expected).only)
              }
          }
        }

      }
    }

    "retrieve values for a given property with a where clause" in sampleData {
      sampleEvents => {
        val eventName = sampleEvents.head.eventName

        val constraintField: JField = sampleEvents.head.data.fields.head
        
        val expected : List[JValue] = sampleEvents.filter {
          ev => ev.eventName == eventName && ev.data.get(constraintField.name) == constraintField.value
        }.map(_.data.value.get("num")).distinct

        val content = """{"where":[{"variable":".%s.%s","value":"%s"}]}""".format(eventName, constraintField.name, constraintField.value.values.toString)
        
        jsonTestService.get[JValue]("/vfs/test/." + eventName + ".num/values?content=" + URLEncoder.encode(content, "UTF-8")) must whenDelivered {
          beLike {
              case HttpResponse(HttpStatus(status, _), _, Some(result), _) => {
                val nonTagData = result.deserialize[List[JValue]].map {
                  case JObject(fields) => JObject(fields.filterNot(_.name.startsWith("#")))
                  case other => other
                }
                (status must_== HttpStatusCodes.OK) and
                (nonTagData must containAllOf(expected).only)
              }
          }
        }
      }
    }

    "retrieve full raw events" in sampleData { 
      sampleEvents => {
        val eventName = sampleEvents.head.eventName

        val expected : List[JValue] = sampleEvents.filter(_.eventName == eventName).map(_.data.value)

        // This also tests to make sure that only the event name (and not properties) are used in the underlying query
        jsonTestService.get[JValue]("/vfs/test/." + eventName + ".dummy/events") must whenDelivered {
          beLike {
              case HttpResponse(HttpStatus(status, _), _, Some(result), _) => {
                val nonTagData = result.deserialize[List[JValue]].map {
                  case JObject(fields) => JObject(fields.filterNot(_.name.startsWith("#")))
                  case other => other
                }
                (status must_== HttpStatusCodes.OK) and
                (nonTagData must haveTheSameElementsAs(expected))
              }
          }
        }
      }
    }

    "retrieve full raw events with a where clause" in sampleData { 
      sampleEvents => {
        val eventName = sampleEvents.head.eventName

        val constraintField: JField = sampleEvents.head.data.fields.head

        val expected : List[JValue] = sampleEvents.filter {
          ev => ev.eventName == eventName && ev.data.get(constraintField.name) == constraintField.value
        }.map(_.data.value)

        val content = """{"where":[{"variable":".%s.%s","value":"%s"}]}""".format(eventName, constraintField.name, constraintField.value.values.toString)

        // This also tests to make sure that only the event name (and not properties) are used in the underlying query
        jsonTestService.get[JValue]("/vfs/test/." + eventName + "/events?content=" + URLEncoder.encode(content, "UTF-8")) must whenDelivered {
          beLike {
              case HttpResponse(HttpStatus(status, _), _, Some(result), _) => {
                val nonTagData = result.deserialize[List[JValue]].map {
                  case JObject(fields) => JObject(fields.filterNot(_.name.startsWith("#")))
                  case other => other
                }
                (status must_== HttpStatusCodes.OK) and
                (nonTagData must containAllOf(expected).only)
              }
          }
        }
      }
    }

    "retrieve full raw events with limit" in sampleData { 
      sampleEvents => {
        val eventName = sampleEvents.head.eventName

        val expected : List[JValue] = sampleEvents.filter(_.eventName == eventName).map(_.data.value)

        jsonTestService.get[JValue]("/vfs/test/." + eventName + "/events?limit=1") must whenDelivered {
          beLike {
              case HttpResponse(HttpStatus(status, _), _, Some(result), _) => {
                (status must_== HttpStatusCodes.OK) and
                (result.deserialize[List[JValue]].length must_== 1)
              }
          }
        }
      }
    }

    "retrieve raw events with select fields" in sampleData {
      sampleEvents => {
        val eventName = sampleEvents.head.eventName

        val fieldName : String = sampleEvents.head.data.fields.head.name

        val expected : List[JValue] = sampleEvents.filter(_.eventName == eventName).map(v => JObject.empty.set(JPath(fieldName), v.data.value \ fieldName))

        jsonTestService.get[JValue]("/vfs/test/.%s/events?properties=.%s".format(eventName, fieldName)) must whenDelivered {
          beLike {
            case HttpResponse(HttpStatus(status, _), _, Some(result), _) => 
              (status must_== HttpStatusCodes.OK) and
              (result.deserialize[List[JValue]] must haveTheSameElementsAs(expected))
          }
        }        
      }
    }
      
    "explore variables" in sampleData { sampleEvents =>
      val expectedChildren = sampleEvents.foldLeft(Map.empty[String, Set[String]]) {
        case (m, Event(eventName, EventData(JObject(fields)), _)) => 
          val properties = fields.map("." + _.name)
          m + (eventName -> (m.getOrElse(eventName, Set.empty[String]) ++ properties))
      }

      expectedChildren forall { 
        case (eventName, children) => 
          (jsonTestService.get[JValue]("/vfs/test/." + eventName)) must whenDelivered {
             beLike {
              case HttpResponse(HttpStatus(status, _), _, Some(result), _) => 
                (status must_== HttpStatusCodes.OK) and
                (result.deserialize[List[String]] must haveTheSameElementsAs(children))
            }
          } 
      }
    }

    "explore tags" in sampleData { sampleEvents =>
      val expectedTags: Set[String] = sampleEvents.flatMap({ case Event(_, _, tags) => tags.map(_.name) })(collection.breakOut)

      (jsonTestService.get[JValue]("/tags/test")) must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(result), _) => 
            (status.code must_== HttpStatusCodes.OK) and
            (result.deserialize[Set[String]] must_== expectedTags)
        }
      }
    }

    "explore the tag hierarchy" in sampleData { sampleEvents =>
      val expectedChildren = sampleEvents.flatMap(_.tags).foldLeft(Map.empty[Path, Set[String]]) {
        case (m, Tag("location", Hierarchy(locations))) => 
          locations.foldLeft(m) {
            (m, l) => l.path.parent match {
              case Some(parent) => m + (parent -> (m.getOrElse(parent, Set.empty[String]) + l.path.elements.last))
              case None => m
            }
          }
        case (m, _) => m
      } 

      forall(expectedChildren) {
        case (hpath, children) => 
          (jsonTestService.get[JValue]("/tags/test/.location." + hpath.elements.mkString("."))) must whenDelivered {
            beLike {
              case HttpResponse(status, _, Some(result), _) => 
                (status.code must_== HttpStatusCodes.OK) and
                (result.deserialize[List[String]] must haveTheSameElementsAs(children))
            }
          }
      }
    }


    "count events by post" in sampleData { sampleEvents =>
      val queryTerms = JObject(JField("location", "usa") :: Nil)

      val counts = sampleEvents.foldLeft(Map.empty[String, Int]) { case (m, Event(name, _, _)) => m + (name -> (m.getOrElse(name, 0) + 1)) }
      counts forall {
        case (name, count) => 
          (jsonTestService.post[JValue]("/vfs/test/."+name+"/count")(queryTerms)) must whenDelivered {
            beLike {
              case HttpResponse(status, _, Some(result), _) => result.deserialize[Long] must_== count
            }
          } 
      }
    }

    "count events by get" in sampleData { sampleEvents =>
      val counts = sampleEvents.foldLeft(Map.empty[String, Int]) { case (m, Event(name, _, _)) => m + (name -> (m.getOrElse(name, 0) + 1)) }
      counts forall {
        case (name, count) => 
          jsonTestService.get[JValue]("/vfs/test/."+name+"/count?location=usa") must whenDelivered {
            beLike {
              case HttpResponse(status, _, Some(result), _) => result.deserialize[Long] must_== count
            }
          } 
      }
    }

    "count events by time range" in sampleData { sampleEvents => {
      val timestamps = sampleEvents.map(_.tags.collect{ case Tag("timestamp", TimeReference(_, time)) => time.getMillis }.head)
      val sortedEvents = sampleEvents.zip(timestamps).sortBy(_._2)

      val (subsetEvents,subsetTimestamps) = sortedEvents.take(sampleEvents.size / 2).unzip
      val counts = subsetEvents.foldLeft(Map.empty[String, Int]) { case (m, Event(name, _, _)) => m + (name -> (m.getOrElse(name, 0) + 1)) }
      val (startTime: Long, endTime: Long) = (subsetTimestamps.head, subsetTimestamps.last)

      counts forall {
        case (name, count) =>
          jsonTestService.get[JValue]("/vfs/test/."+name+"/count?start="+startTime+"&end="+(endTime + 1)) must whenDelivered {
            beLike {
              case HttpResponse(status, _, Some(result), _) => {
                if (result.deserialize[Long] != count) { println("Mismatch on " + name) }
                result.deserialize[Long] must_== count
              }
            }
          } 
      }
    }}
      

    "not roll up by default" in {
      jsonTestService.get[JValue]("/vfs/.tweeted/count?location=usa") must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(result), _) => result.deserialize[Long] must_== 0l
        }
      } 
    }

    "return variable counts keyed by tag children" in sampleData { sampleEvents =>
      val (events, minDate, maxDate, granularity) = timeSlice(sampleEvents)

      val expectedResults = events.foldLeft(Map.empty[String, Map[String, Int]]) {
        case (acc, Event(eventName, _, tags)) =>
          val state = tags.collect({ case Tag("location", Hierarchy(locations)) => locations.find(_.path.length == 2).map(_.path.path) }).flatten.head
          val locationCounts = acc.getOrElse(eventName, Map.empty[String, Int])
          acc + (eventName -> (locationCounts + (state -> (locationCounts.getOrElse(state, 0) + 1))))
      }

      val queryTerms = JObject(
        JField("start", minDate.getMillis) ::
        JField("end", maxDate.getMillis) ::
        JField("location", "usa") :: Nil
      )

      forall(expectedResults) {
        case (eventName, locationCounts) =>
          (jsonTestService.post[JValue]("/vfs/test/." + eventName + ".recipientCount/count?use_tag_children=location")(queryTerms)) must whenDelivered {
            beLike {
              case HttpResponse(status, _, Some(contents), _) => 
                (status.code must_== HttpStatusCodes.OK) and
                forall(locationCounts) {
                  case (key, count) => (((contents --> classOf[JObject]) \ key).deserialize[Int] must_== count)
                }
            }
          }
      }
    }

    "return variable series means" in sampleData { sampleEvents =>
      val (events, minDate, maxDate, granularity) = timeSlice(sampleEvents)
      val expected = expectedMeans(events, "recipientCount", keysf(granularity))

      val queryTerms = JObject(
        JField("start", minDate.getMillis) ::
        JField("end", maxDate.getMillis) ::
        JField("location", "usa") :: Nil
      )

      (jsonTestService.post[JValue]("/vfs/test/.tweeted.recipientCount/series/"+granularity.name+"/means")(queryTerms)) must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(contents), _) => 
            val resultData = (contents: @unchecked) match {
              case JArray(values) => values.collect { 
                case JArray(List(JObject(List(JField("timestamp", k), JField("location", k2))), JDouble(v))) => 
                  (List(k.deserialize[Instant].toString, k2.deserialize[String]), v)
              }
            }

            // TODO: Should fix data generation so that we always have "tweeted" events
            resultData.toMap must haveTheSameElementsAs(expected.get("tweeted").getOrElse(Map()))
        }
      } 
    }

    "return variable series means using a where clause" in sampleData { sampleEvents =>
      val desiredTwitterClient : JString = sampleEvents.head.data.value.get("twitterClient") match {
        case js : JString => js
        case other => JString("broken!")
      }

      val (events, minDate, maxDate, granularity) = timeSlice(sampleEvents.filter {
        case Event(_, EventData(obj), _) => obj.get("twitterClient") == desiredTwitterClient
      })

      val expected = expectedMeans(events, "recipientCount", keysf(granularity))

      val queryTerms = JObject(
        JField("start", minDate.getMillis) ::
        JField("end", maxDate.getMillis) ::
        JField("location", "usa") :: 
        JField("where", JArray(List(JObject(JField("variable", ".tweeted.twitterClient") :: JField("value", desiredTwitterClient) :: Nil)))) :: Nil
      )

      (jsonTestService.post[JValue]("/vfs/test/.tweeted.recipientCount/series/"+granularity.name+"/means")(queryTerms)) must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(contents), _) => 
            val resultData = (contents: @unchecked) match {
              case JArray(values) => values.collect { 
                case JArray(List(JObject(List(JField("timestamp", k), JField("location", k2))), JDouble(v))) => 
                  (List(k.deserialize[Instant].toString, k2.deserialize[String]), v)
              }
            }

            resultData.toMap must haveTheSameElementsAs(expected.get("tweeted").getOrElse(Map()))
        }
      } 
    }

    "return variable value series counts" in sampleData { sampleEvents =>
      val (events, minDate, maxDate, granularity) = timeSlice(sampleEvents)
      val expectedTotals = valueCounts(events) 

      val queryTerms = JObject(
        JField("start", minDate.getMillis) ::
        JField("end", maxDate.getMillis) ::
        JField("location", "usa") :: Nil
      )

      forallWhen(expectedTotals) {
        case ((jpath, value), count) if jpath.nodes.last == JPathField("gender") && !jpath.endsInInfiniteValueSpace =>
          val vtext = compact(render(value))
          val servicePath = "/vfs/test/"+jpath+"/values/"+vtext+"/series/"+granularity.name
          (jsonTestService.post[JValue](servicePath)(queryTerms)) must whenDelivered {
            beLike {
              case HttpResponse(status, _, Some(JArray(values)), _) => (values must not be empty) //and (series must_== expected)
            }
          }
      }
    }

    "group variable value series counts" in (sampleData { sampleEvents =>
      val (events, minDate, maxDate, granularity) = timeSlice(sampleEvents)
      val expectedTotals = valueCounts(events) 

      val queryTerms = JObject(
        JField("start", minDate.getMillis) ::
        JField("end", maxDate.getMillis) ::
        JField("location", "usa") :: Nil
      )

      forallWhen(expectedTotals) {
        case ((jpath, value), count) if jpath.nodes.last == JPathField("gender") && !jpath.endsInInfiniteValueSpace =>
          val vtext = compact(render(value))
          val servicePath = "/vfs/test/"+jpath+"/values/"+vtext+"/series/"+granularity.name+"?groupBy="+granularity.next.next.name
          (jsonTestService.post[JValue](servicePath)(queryTerms)) must whenDelivered {
            beLike {
              case HttpResponse(status, _, Some(JArray(values)), _) => (values must not be empty) //and (series must_== expected)
            }
          }
      }
    })

    "intersection series must sum to count over the same period" in sampleData { sampleEvents =>
      val (events, minDate, maxDate, granularity) = timeSlice(sampleEvents)
      val expectedTotals = valueCounts(events) 

      val baseQuery = """{
        "start": %1d,
        "end": %1d,
        "select":"%s",
        "from":"/test/",
        "properties":[{"property":".tweeted.gender","limit":10,"order":"descending"}]
      }"""
      
      def countResponse = jsonTestService.post[JValue]("/intersect")(JsonParser.parse(baseQuery.format(minDate.getMillis, maxDate.getMillis, "count"))) map {
        case HttpResponse(_, _, Some(content), _) => content
      }

      def seriesResponse = jsonTestService.post[JValue]("/intersect")(JsonParser.parse(baseQuery.format(minDate.getMillis, maxDate.getMillis, "series/"+granularity.name))) map {
        case HttpResponse(_, _, Some(JObject(fields)), _) => JObject(
          fields map {
            case JField(label, JArray(values)) => JField(label, values.foldLeft(0L) { case (total, JArray(_ :: count :: Nil)) => total + count.deserialize[Long] }.serialize)
          }
        )
      }

      (countResponse zip seriesResponse) must whenDelivered {
        beLike {
          case (a, b) => (a must_!= 0) and (a must_== b)
        }
      }
    }

    "intersection series must sum to count over the same period with a where clause" in sampleData { sampleEvents =>
      val desiredTwitterClient : JString = sampleEvents.head.data.value.get("twitterClient") match {
        case js : JString => js
        case other => JString("broken!")
      }

      val (events, minDate, maxDate, granularity) = timeSlice(sampleEvents.filter {
        case Event(_, EventData(obj), _) => obj.get("twitterClient") == desiredTwitterClient
      })

      val expectedTotals = valueCounts(events) 

      val baseQuery = """{
        "start": %1d,
        "end": %1d,
        "select":"%s",
        "from":"/test/",
        "properties":[{"property":".tweeted.gender","limit":10,"order":"descending"}],
        "where":[{"variable":".tweeted.twitterClient","value":"%s"}]
      }"""
      
      def countResponse = jsonTestService.post[JValue]("/intersect")(JsonParser.parse(baseQuery.format(minDate.getMillis, maxDate.getMillis, "count", desiredTwitterClient.values))) map {
        case HttpResponse(_, _, Some(content), _) => content
      }

      def seriesResponse = jsonTestService.post[JValue]("/intersect")(JsonParser.parse(baseQuery.format(minDate.getMillis, maxDate.getMillis, "series/"+granularity.name, desiredTwitterClient.values))) map {
        case HttpResponse(_, _, Some(JObject(fields)), _) => JObject(
          fields map {
            case JField(label, JArray(values)) => JField(label, values.foldLeft(0L) { case (total, JArray(_ :: count :: Nil)) => total + count.deserialize[Long] }.serialize)
          }
        )
      }

      (countResponse zip seriesResponse) must whenDelivered {
        beLike {
          case (a, b) => (a must_!= 0) and (a must_== b)
        }
      }
    }

    "grouping in intersection queries" >> {
      "timezone shifting must not discard data" in sampleData { sampleEvents =>
        val (events, minDate, maxDate, granularity) = timeSlice(sampleEvents)
        val queryGranularity = granularity.next.next

        val servicePath1 = "/intersect?start=" + minDate.getMillis + "&end=" + maxDate.getMillis + "&timeZone=-5.0&groupBy="+queryGranularity.name
        val servicePath2 = "/intersect?start=" + minDate.getMillis + "&end=" + maxDate.getMillis + "&timeZone=-4.0&groupBy="+queryGranularity.name
        
        val queryTerms = JsonParser.parse(
          """{
            "select":"series/%s",
            "from":"/test/",
            "properties":[{"property":".tweeted.recipientCount","limit":10,"order":"descending"}]
          }""".format(granularity.name)
        )

        val q1Results = jsonTestService.post[JValue](servicePath1)(queryTerms) 
        val q2Results = jsonTestService.post[JValue](servicePath2)(queryTerms) 

        (q1Results zip q2Results) must whenDelivered {
          beLike { 
            case (r1, r2) => 
              r2.content must matchShiftedHistogram(r1.content, 1, granularity)
          }
        }
      }
    }
  }
}

case class matchShiftedHistogram(o: Option[JValue], hourOffset: Int, granularity: Periodicity, threshold: Double = 0.9) extends Matcher[Option[JValue]]() {

  import Periodicity._

  val testConfig: Map[Periodicity, Int] = Map(
    Minute -> 60 * 60,
    Second -> 60,
    Hour   -> 1,
    Day    -> 0,
    Week   -> 0,
    Month  -> 0,
    Year   -> 0
  )

  def convertToMap(opt: Option[JValue]): Map[String, Map[BigInt, BigInt]] = {
    opt.get.children.foldLeft(Map[String, Map[BigInt, BigInt]]())((m, v) => v match {
      case JField(name, value) => {
        m + (name -> (value --> classOf[JArray]).children.foldLeft(Map[BigInt, BigInt]())((im, iv) => iv match {
          case JArray(JObject(JField(name, JInt(id)) :: Nil) :: JInt(count) :: Nil) => im + (id -> count)
          case _                                                                    => sys.error("Response format changed unit tests needs to be updated.")
        }))
      }
      case _                   => sys.error("Response format changed unit tests needs to be updated.")
    })
  }

  def apply[T <: Option[JValue]](e: Expectable[T]): MatchResult[T] = {
    val h1 = convertToMap(e.value)
    val h2 = convertToMap(o)

    val offset = testConfig(granularity) * hourOffset

    val paired = for ((k1, v1) <- h1.toSeq; v2 <- h2.get(k1)) yield (v1, v2)
    if (paired.size != h1.size) result(false, "Results have the same keys.", "Results do not have the same keys (This is likely due to a response format change).", e)
    else {
      val pass = paired map { 
        case (a, b) => { 
          val hits = a count { case (k, v) => b.get(k - offset).exists(_ == v) }
          hits / a.size.toDouble
        }
      } forall {
        _ > threshold
      }

      result(pass, "Histograms are shifted versions of one another", "Histograms are not shifted versions of one another", e) 
    }
  }
}

class RootTrackingAnalyticsServiceSpec extends TestAnalyticsService with ArbitraryEvent with FutureMatchers {
  override val genTimeClock = clock 

  object sampleData extends Outside[List[Event]] with Scope {
    val outside = containerOfN[List, Event](10, fullEventGen).sample.get ->- {
      _.foreach(event => jsonTestService.post[JValue]("/vfs/")(event.message))
    }
  }

  "When writing to the service root" should {
    "count events by get" in sampleData { sampleEvents =>
      lazy val tweetedCount = sampleEvents.count {
        case Event("tweeted", _, _) => true
        case _ => false
      }

      jsonTestService.get[JValue]("/vfs/.tweeted/count?location=usa") must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(result), _) => result.deserialize[Long] must_== tweetedCount
        }
      } 
    }

    "retrieve path children at the root" in {
      jsonTestService.get[JValue]("/vfs/?location=usa") must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(JArray(elements)), _) => (elements collect { case JString(s) => s }) must contain(".tweeted")
        }
      } 
    }

    "retrieve property children at the root" in {
      jsonTestService.get[JValue]("/vfs/.tweeted?location=usa") must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(JArray(elements)), _) => (elements collect { case JString(s) => s }) must contain(".twitterClient")
        }
      } 
    }
  }
}

class SingleTokenPathAnalyticsServiceSpec extends TestAnalyticsService with ArbitraryEvent with FutureMatchers {
  override val genTimeClock = clock 

  object sampleData extends Outside[List[Event]] with Scope {
    val outside = containerOfN[List, Event](10, fullEventGen).sample.get ->- {
      _.foreach(event => jsonTestService.post[JValue]("/vfs/t")(event.message))
    }
  }

  "When writing to the a single token path" should {
    "count events by get" in sampleData { sampleEvents =>
      lazy val tweetedCount = sampleEvents.count {
        case Event("tweeted", _, _) => true
        case _ => false
      }

      jsonTestService.get[JValue]("/vfs/t/.tweeted/count?location=usa") must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(result), _) => result.deserialize[Long] must_== tweetedCount
        }
      } 
    }

    "retrieve path children" in {
      jsonTestService.get[JValue]("/vfs/t?location=usa") must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(JArray(elements)), _) => (elements collect { case JString(s) => s }) must contain(".tweeted")
        }
      } 
    }

    "retrieve property children" in {
      jsonTestService.get[JValue]("/vfs/t/.tweeted?location=usa") must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(JArray(elements)), _) => (elements collect { case JString(s) => s }) must contain(".twitterClient")
        }
      } 
    }
  }
}

class RollupAnalyticsServiceSpec extends TestAnalyticsService with ArbitraryEvent with FutureMatchers {
  override val genTimeClock = clock 

  object sampleData extends Outside[List[Event]] with Scope {
    val outside = containerOfN[List, Event](30, fullEventGen).sample.get ->- {
      _.foreach(event => jsonTestService.query("rollup", "1").post[JValue]("/vfs/test/foo.bar%40baz.com/tags/foo/")(event.message))
    }
  }

  "Analytics Service" should {
    "roll up data to expected parent paths" in sampleData { sampleEvents =>
      lazy val tweetedCount = sampleEvents.count {
        case Event("tweeted", _, _) => true
        case _ => false
      }

      jsonTestService.get[JValue]("/vfs/test/foo.bar%40baz.com/tags/foo/.tweeted/count") must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(result), _) => {
            result.deserialize[Long] must_== tweetedCount
          }
        }
      }
    }
    
    "roll up data to expected parent paths with tags query" in sampleData { sampleEvents =>
      lazy val tweetedCount = sampleEvents.count {
        case Event("tweeted", _, _) => true
        case _ => false
      }

      jsonTestService.get[JValue]("/vfs/test/foo.bar%40baz.com/tags/foo/.tweeted/count?location=usa") must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(result), _) => {
            result.deserialize[Long] must_== tweetedCount
          }
        }
      }
    }
    
    "do not roll up data beyond expected parent paths" in sampleData { sampleEvents =>
      lazy val tweetedCount = sampleEvents.count {
        case Event("tweeted", _, _) => true
        case _ => false
      }

      jsonTestService.get[JValue]("/vfs/test/foo.bar%40baz.com/.tweeted/count") must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(result), _) => {
            result.deserialize[Long] must_== 0 
          }
        }
      }
    }
    
    "do not roll up data beyond expected parent paths with tags query" in sampleData { sampleEvents =>
      lazy val tweetedCount = sampleEvents.count {
        case Event("tweeted", _, _) => true
        case _ => false
      }

      jsonTestService.get[JValue]("/vfs/test/foo.bar%40baz.com/.tweeted/count?location=usa") must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(result), _) => {
            result.deserialize[Long] must_== 0 
          }
        }
      }
    }

    "correctly handle rollup in intersection queries" in sampleData { sampleEvents =>
      val (events, minDate, maxDate, granularity) = timeSlice(sampleEvents)

      val expectedCounts = events.foldLeft(Map.empty[String, Map[String, Int]]) {
        case (map, Event("tweeted", data, _)) =>
          val gender = renderNormalized((data \ "gender").deserialize[String])
          val recipCount = (data \ "recipientCount").deserialize[String]
          val countMap = map.getOrElse(gender, Map.empty[String, Int])
          
          map + (gender -> (countMap + (recipCount -> (countMap.getOrElse(recipCount, 0) + 1))))
        case (map, _) => map
      }.serialize

      val leaf = jsonTestService.post[JValue]("/intersect?start=" + minDate.getMillis + "&end=" + maxDate.getMillis) {
        JsonParser.parse("""{
          "select":"count",
          "from":"/test/foo.bar@baz.com/tags/foo/",
          "properties":[
            {"property":".tweeted.gender", "limit":10,"order":"ascending"},
            {"property":".tweeted.recipientCount","limit":10,"order":"descending"}
          ]
        }""")
      } 
      
      val root = jsonTestService.post[JValue]("/intersect?start=" + minDate.getMillis + "&end=" + maxDate.getMillis) {
        JsonParser.parse("""{
          "select":"count",
          "from":"/test/foo.bar@baz.com/tags/",
          "properties":[
            {"property":".tweeted.gender", "limit":10,"order":"ascending"},
            {"property":".tweeted.recipientCount","limit":10,"order":"descending"}
          ]
        }""")
      } 

      (leaf zip root) must whenDelivered {
        beLike {
          case (HttpResponse(leafStatus, _, Some(leafResult), _), HttpResponse(rootStatus, _, Some(rootResult), _)) => 
            logger.trace("Leaf = " + leafResult)
            logger.trace("Root = " + rootResult)
            (expectedCounts must_== leafResult) and 
            (leafResult must_== rootResult)
        }
      }
    }
  }
}

class VariantPathAnalyticsServiceSpec extends TestAnalyticsService with ArbitraryEvent with FutureMatchers {
  override val genTimeClock = clock 

  object sampleData extends Outside[List[Event]] with Scope {
    def outside = containerOfN[List, Event](10, fullEventGen).sample.get ->- {
      _.foreach(event => jsonTestService.post[JValue]("/vfs/test/foo.bar%40baz.com")(event.message))
    }
  }

  "Analytics Service" should {
    "handle data with " in sampleData { sampleEvents =>
      lazy val tweetedCount = sampleEvents.count {
        case Event("tweeted", _, _) => true
        case _ => false
      }

      jsonTestService.get[JValue]("/vfs/test/foo.bar%40baz.com/.tweeted/count?location=usa") must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(result), _) => result.deserialize[Long] must_== tweetedCount
        }
      }
    }
  }
}

class UnicodeAnalyticsServiceSpec extends TestAnalyticsService with ArbitraryEvent with FutureMatchers {
  override val genTimeClock = clock 

  implicit object JsonStringBijection extends Bijection[String, JValue] {  
    def apply(s: String): JValue   = JsonParser.parse(s)
    def unapply(t: JValue): String = compact(render(t))
  }

  "Analytics Service" should {
    "accept events containing unicode" in {
      val eventData = """
        {"case":{"sourceType":2,"os":"Win","browser":"MSIE9","fullUrl":"usbeds.com/Brand/Simmons%C2%AE/Beautyrest%C2%AE_Classic%E2%84%A2.aspx","entryUrl":"usbeds.com/Products/Simmons®_Beautyrest®_Classic™_Mercer_Park™_Plush_Pillow_Top_","referrerUrl":"google.com","agentName":"Jason","agentId":"jbircann@olejo.com","chatDuration":473,"chatResponseTime":0,"chatResponded":true,"#location":{"country":"United States","region":"United States/IL","city":"United States/IL/Deerfield"},"searchKeyword":{},"#timestamp":""" + clock.instant().getMillis.toString + """}}
      """

      jsonTestService.query("rollup", "0").post[String]("/vfs/test")(eventData) must whenDelivered {
        beLike {
          case HttpResponse(HttpStatus(status, _), _, _, _) => 
            (status must_== HttpStatusCodes.OK) and
            (jsonTestService.get[JValue]("/vfs/test/.case.os/values?location=United%20States") must whenDelivered {
              beLike {
                case HttpResponse(HttpStatus(HttpStatusCodes.OK, _), _, Some(result), _) => result.deserialize[List[String]] must_== List("Win")
              }
            }) 
        }
      }
    }
  }
}

//// We no longer trat archival events differently now that we use raw events for queries
//class ArchivalAnalyticsServiceSpec extends TestAnalyticsService with ArbitraryEvent with FutureMatchers {
//  import org.scalacheck.Gen
//
//  override val genTimeClock = PastClock(Days.days(1).toStandardDuration)
//
//  // Overriding so that we generate events on either side of 24 hours ago
//  override val genTime = {
//    for {
//      periodicities <- pick(3, genTimePeriodicities)
//      direction <- Gen.oneOf(List(-1,1))
//      offsets <- periodicities map { case (periodicity, max) => choose(0, max).map{ offval => periodicity.jodaPeriod(offval  * direction).get }} sequence
//    } yield {
//      offsets.foldLeft(genTimeClock.now()) { (date, offset) => date.plus(offset) } toInstant
//    }
//  }
//
//  object sampleData extends Outside[List[Event]] with Scope {
//    def outside = containerOfN[List, Event](10, fullEventGen).sample.get ->- {
//      _.foreach(event => jsonTestService.post[JValue]("/vfs/test")(event.message))
//    }
//  }
//
//  "Analytics Service" should {
//    "store archived events (> 1 day) in the events database, but not in the index." in sampleData { sampleEvents =>
//      val (beforeCutoff, afterCutoff) = sampleEvents.partition(_.timestamp.exists(_ <= clock.now.minusDays(1)))
//
//      lazy val tweetedCount = afterCutoff.count {
//        case Event("tweeted", _, _) => true
//        case _ => false
//      }
//
//      (beforeCutoff must not be empty) and 
//      (afterCutoff must not be empty) and 
//      (jsonTestService.get[JValue]("/vfs/test/.tweeted/count?location=usa") must whenDelivered {
//        beLike {
//          case HttpResponse(status, _, Some(result), _) => result.deserialize[Long] must_== tweetedCount
//        }
//      }) 
//    }
//  }
//}

class StorageReportingAnalyticsServiceSpec extends TestAnalyticsService with ArbitraryEvent with FutureMatchers {
  override val genTimeClock = Clock.System

  val testStart = genTimeClock.now
  
  lazy val trackingTestService = service.contentType[JValue](application/(MimeTypes.json)).
                                     query("tokenId", TrackingToken.tokenId)
  
  val testValues = 1.until(9).map("val" + _)

  val testData = testValues.map(v => Event("track", EventData(JObject(List(JField("prop", v)))), List[Tag]())).toList

  object simpleSampleData extends Outside[Event] with Scope {
    def outside = testData(0) ->- { e: Event =>
      jsonTestService.post[JValue]("/vfs/test")(e.message)
    }
  }
 

  object sampleData extends Outside[List[Event]] with Scope {
    def outside = testData.slice(1, testData.length) ->- {
      _.foreach(event => jsonTestService.post[JValue]("/vfs/test")(event.message))
    }
  }
 
  def histogramTotal(jval: JValue): Long = {
    val outer = jval.asInstanceOf[JArray]
    outer.values.foldLeft(0: Long){ (sum, e) => {
      sum + (e match {
        case value :: count :: Nil => value.asInstanceOf[BigInt].toLong * count.asInstanceOf[BigInt].toLong
        case _                      => {
          sys.error("Invalid histogram result")
        }
      })
    }}
  }

  "Storage report" should {
    "a single track should produce a single count" in simpleSampleData { sampleEvent =>
      (jsonTestService.get[JValue]("/vfs/test/.track/count") must whenDelivered {
        beLike {
          case HttpResponse(HttpStatus(HttpStatusCodes.OK, _), _, Some(result), _) => result.deserialize[Long] must_== 1
        }
      }) and {
        trackingTestService.get[JValue]("/vfs/unittest/.stored/count") must whenDelivered {
          beLike {
            case HttpResponse(HttpStatus(HttpStatusCodes.OK, _), _, Some(result), _) => result.deserialize[Long] must_== 1
          }
        }
      } and {
        trackingTestService.get[JValue]("/vfs/unittest/.stored.count/histogram") must whenDelivered {
          beLike {
            case HttpResponse(HttpStatus(HttpStatusCodes.OK, _), _, Some(result), _) => {
              histogramTotal(result) must_== 1
            }
          }
        }
      }
    }.pendingUntilFixed("!!! Need a refactor on storage reporting")

    "multiple tracks should create a matching number of counts" in sampleData { sampleEvents =>
      (jsonTestService.get[JValue]("/vfs/test/.track/count") must whenDelivered {
        beLike {
          case HttpResponse(HttpStatus(HttpStatusCodes.OK, _), _, Some(result), _) => result.deserialize[Long] must_== testData.length
        }
      }) and {
        trackingTestService.get[JValue]("/vfs/unittest/.stored/count") must whenDelivered {
          beLike {
            case HttpResponse(HttpStatus(HttpStatusCodes.OK, _), _, Some(result), _) => {
              result.deserialize[Int] must be_>(0) and be_<=(testData.length)
            }
          }
        }
      } and {
        trackingTestService.get[JValue]("/vfs/unittest/.stored.count/histogram") must whenDelivered {
          beLike {
            case HttpResponse(HttpStatus(HttpStatusCodes.OK, _), _, Some(result), _) => {
              histogramTotal(result) must_== testData.length 
            }
          }
        }
      }
    }.pendingUntilFixed("!!! Need a refactor on storage reporting")

    def timeBoundedUsageSums(tokenId: String, path: String, start: DateMidnight, end: DateMidnight): Future[HttpResponse[JValue]] = {
      val client = service.contentType[JValue](application/(MimeTypes.json))
                                     .query("tokenId", tokenId)
                                     .query("start", start.getMillis.toString)
                                     .query("end", end.getMillis.toString)
   
      client.get[JValue]("/vfs/" + path + "/series/day/sums")
    }

    def reduceUsageSums(jval: JValue): Long = {
      jval match {
        case JArray(Nil) => 0l
        case JArray(l)   => l.map {
          case JArray(ts :: JDouble(c) :: Nil ) => c.toLong
          case JArray(ts :: v :: Nil)         => 0l
          case _                              => sys.error("Unexpected series result format")
        }.reduce(_ + _)
        case _           => sys.error("Error parsing usage count.")
      }
    }

    "time bounded histogram includes expected counts" in { 
      val today = new DateTime(DateTimeZone.UTC).toDateMidnight

      timeBoundedUsageSums(TrackingToken.tokenId, "unittest/.stored.count", 
                           today, today.plusDays(1)) must whenDelivered {
        beLike {
          case HttpResponse(HttpStatus(HttpStatusCodes.OK, _), _, Some(result), _) => {
            reduceUsageSums(result) must_== 8
          }
        }
      }
    }.pendingUntilFixed("!!! Need a refactor on storage reporting")
    "time bounded histogram excludes expected counts" in { 
      skipped("This test fails to fail on missing data")
      val today = new DateTime(DateTimeZone.UTC).toDateMidnight

      timeBoundedUsageSums(TrackingToken.tokenId, "unittest/.stored.count", 
                           today.minusDays(1), today) must whenDelivered {
        beLike {
          case HttpResponse(HttpStatus(HttpStatusCodes.OK, _), _, Some(result), _) => {
            reduceUsageSums(result) must_== 0
          }
        }
      }
    }
  }
}
