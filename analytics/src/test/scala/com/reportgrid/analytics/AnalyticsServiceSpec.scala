package com.reportgrid
package analytics

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
        val latch = new java.util.concurrent.CountDownLatch(1)
        val result = service.apply(HttpRequest(httpMethod, url, Map(), headers, chunkContent)).deliverTo(_ => latch.countDown())
        latch.await()
        result.value.flatMap(_.content.map(ChunkToString)).getOrElse("")
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
    val outside = containerOfN[List, Event](10, fullEventGen).sample.get ->- {
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

    "not roll up by default" in {
      jsonTestService.get[JValue]("/vfs/.tweeted/count?location=usa") must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(result), _) => result.deserialize[Long] must_== 0l
        }
      } 
    }

    "return variable series means" in sampleData { sampleEvents =>
      val (events, minDate, maxDate) = timeSlice(sampleEvents, Hour)
      val expected = expectedMeans(events, "recipientCount", keysf(Hour))

      val queryTerms = JObject(
        JField("start", minDate.getMillis) ::
        JField("end", maxDate.getMillis) ::
        JField("location", "usa") :: Nil
      )

      (jsonTestService.post[JValue]("/vfs/test/.tweeted.recipientCount/series/hour/means")(queryTerms)) must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(contents), _) => 
            val resultData = (contents: @unchecked) match {
              case JArray(values) => values.collect { 
                case JArray(List(JObject(List(JField("timestamp", k), JField("location", k2))), JDouble(v))) => 
                  (List(k.deserialize[Instant].toString, k2.deserialize[String]), v)
              }
            }

            resultData.toMap must haveTheSameElementsAs(expected("tweeted"))
        }
      } 
    }

    "return variable value series counts" in sampleData { sampleEvents =>
      val granularity = Hour
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)
      val expectedTotals = valueCounts(events) 

      val queryTerms = JObject(
        JField("start", minDate.getMillis) ::
        JField("end", maxDate.getMillis) ::
        JField("location", "usa") :: Nil
      )

      forallWhen(expectedTotals) {
        case ((jpath, value), count) if jpath.nodes.last == JPathField("gender") && !jpath.endsInInfiniteValueSpace =>
          val vtext = compact(render(value))
          val servicePath = "/vfs/test/"+jpath+"/values/"+vtext+"/series/hour"
          (jsonTestService.post[JValue](servicePath)(queryTerms)) must whenDelivered {
            beLike {
              case HttpResponse(status, _, Some(JArray(values)), _) => (values must not be empty) //and (series must_== expected)
            }
          }
      }
    }

    "group variable value series counts" in (sampleData { sampleEvents =>
      val granularity = Hour
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)
      val expectedTotals = valueCounts(events) 

      val queryTerms = JObject(
        JField("start", minDate.getMillis) ::
        JField("end", maxDate.getMillis) ::
        JField("location", "usa") :: Nil
      )

      forallWhen(expectedTotals) {
        case ((jpath, value), count) if jpath.nodes.last == JPathField("gender") && !jpath.endsInInfiniteValueSpace =>
          val vtext = compact(render(value))
          val servicePath = "/vfs/test/"+jpath+"/values/"+vtext+"/series/hour?groupBy=day"
          (jsonTestService.post[JValue](servicePath)(queryTerms)) must whenDelivered {
            beLike {
              case HttpResponse(status, _, Some(JArray(values)), _) => (values must not be empty) //and (series must_== expected)
            }
          }
      }
    })//.pendingUntilFixed

    "grouping in intersection queries" >> {
      "timezone shifting must not discard data" in (sampleData { sampleEvents =>
        val granularity = Hour
        val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)

        val servicePath1 = "/intersect?start=" + minDate.getMillis + "&end=" + maxDate.getMillis + "&timeZone=-5.0&groupBy=week"
        val servicePath2 = "/intersect?start=" + minDate.getMillis + "&end=" + maxDate.getMillis + "&timeZone=-4.0&groupBy=week"
        val queryTerms = JsonParser.parse(
          """{
            "select":"series/hour",
            "from":"/test/",
            "properties":[{"property":".tweeted.recipientCount","limit":10,"order":"descending"}]
          }"""
        )

        val q1Results = jsonTestService.post[JValue](servicePath1)(queryTerms) 
        val q2Results = jsonTestService.post[JValue](servicePath2)(queryTerms) 

        (q1Results zip q2Results) must whenDelivered {
          beLike { 
            case (r1, r2) => 
              r2.content must be_!=(r1.content)
          }
        }
      })//.pendingUntilFixed
    }
  }
}

class RootTrackingAnalyticsServiceSpec extends TestAnalyticsService with ArbitraryEvent with FutureMatchers {
  override val genTimeClock = clock 

  object sampleData extends Outside[List[Event]] with Scope {
    def outside = containerOfN[List, Event](10, fullEventGen).sample.get ->- {
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
    def outside = containerOfN[List, Event](10, fullEventGen).sample.get ->- {
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
    def outside = containerOfN[List, Event](10, fullEventGen).sample.get ->- {
      _.foreach(event => jsonTestService.query("rollup", "2").post[JValue]("/vfs/test")(event.message))
    }
  }

  "Analytics Service" should {
    "roll up data to parent paths" in sampleData { sampleEvents =>
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

      jsonTestService.get[JValue]("/vfs/test/foo.bar%40baz/.tweeted/count?location=usa") must whenDelivered {
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
            (jsonTestService.get[JValue]("/vfs/test/.case.os/count?location=United%20States") must whenDelivered {
              beLike {
                case HttpResponse(HttpStatus(HttpStatusCodes.OK, _), _, Some(result), _) => result.deserialize[Long] must_== 1
              }
            }) 
        }
      }
    }
  }
}

class ArchivalAnalyticsServiceSpec extends TestAnalyticsService with ArbitraryEvent with FutureMatchers {
  override val genTimeClock = PastClock(Days.TWO.toStandardDuration)

  object sampleData extends Outside[List[Event]] with Scope {
    def outside = containerOfN[List, Event](10, fullEventGen).sample.get ->- {
      _.foreach(event => jsonTestService.post[JValue]("/vfs/test")(event.message))
    }
  }

  "Analytics Service" should {
    "store events in the events database, but not in the index." in sampleData { sampleEvents =>
      val (beforeCutoff, afterCutoff) = sampleEvents.partition(_.timestamp.exists(_ <= clock.now.minusDays(1)))

      lazy val tweetedCount = afterCutoff.count {
        case Event("tweeted", _, _) => true
        case _ => false
      }

      (beforeCutoff must not be empty) and 
      (afterCutoff must not be empty) and 
      (jsonTestService.get[JValue]("/vfs/test/.tweeted/count?location=usa") must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(result), _) => result.deserialize[Long] must_== tweetedCount
        }
      }) 
    }
  }
}

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
    }
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
    }

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
    }
    "time bounded histogram excludes expected counts" in { 
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
