package com.reportgrid.analytics

import blueeyes._
import blueeyes.core.data._
import blueeyes.core.data.Bijection.identity
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.concurrent.test._
import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.xschema.JodaSerializationImplicits._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.JPathImplicits._
import blueeyes.persistence.mongo.{Mongo, RealMongo, MockMongo}
import blueeyes.util.metrics.Duration._
import blueeyes.util.Clock
import MimeTypes._

import org.joda.time._
import net.lag.configgy.ConfigMap

import org.specs._
import org.specs.specification.PendingUntilFixed
import org.scalacheck.Gen._
import scalaz.Success
import scalaz.Scalaz._

//import rosetta.json.blueeyes._

import Periodicity._
import AggregationEngine.ResultSet
import persistence.MongoSupport._
import com.reportgrid.ct._
import com.reportgrid.ct.Mult._
import com.reportgrid.ct.Mult.MDouble._

import BijectionsChunkJson._
import BijectionsChunkString._
import BijectionsChunkFutureJson._

case class PastClock(duration: Duration) extends Clock {
  def now() = new DateTime().minus(duration)
  def instant() = now().toInstant
  def nanoTime = sys.error("nanotime not available in the past")
}

trait TestAnalyticsService extends BlueEyesServiceSpecification with AnalyticsService with LocalMongo {
  val requestLoggingData = """
    requestLog {
      enabled = true
      fields = "time cs-method cs-uri sc-status cs-content"
    }
  """

  override val clock = Clock.System
  override val configuration = "services{analytics{v1{" + requestLoggingData + mongoConfigFileData + "}}}"

  //override def mongoFactory(config: ConfigMap): Mongo = new RealMongo(config)
  override def mongoFactory(config: ConfigMap): Mongo = new MockMongo()

  def auditClient(config: ConfigMap) = external.NoopTrackingClient
  def jessup(configMap: ConfigMap) = external.Jessup.Noop

  lazy val jsonTestService = service.contentType[JValue](application/(MimeTypes.json)).
                                     query("tokenId", Token.Test.tokenId)

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(40, 1000L.milliseconds)
}

class AnalyticsServiceSpec extends TestAnalyticsService with ArbitraryEvent with FutureMatchers with PendingUntilFixed {
  override val genTimeClock = clock 

  "Analytics Service" should {
    shareVariables()

    val sampleEvents: List[Event] = containerOfN[List, Event](10, fullEventGen).sample.get ->- {
      _.foreach(event => jsonTestService.post[JValue]("/vfs/test")(event.message))
    }

    "create child tokens without a trailing slash" in {
        val newToken = Token.Test.issue(permissions = Permissions(read = true, write = true, share = false, explore = false))
        jsonTestService.post[JValue]("/tokens")(newToken.serialize) must whenDelivered {
          beLike {
            case HttpResponse(HttpStatus(status, _), _, Some(JString(tokenId)), _) => 
              val overrideFutureTimeouts = FutureTimeouts(5, 50.milliseconds)

              (status must_== HttpStatusCodes.OK) && 
              (tokenId.length must_== Token.Test.tokenId.length) &&
              (jsonTestService.get[JValue]("/tokens") must whenDelivered({
                beLike[HttpResponse[JValue]] {
                  case HttpResponse(status, _, Some(JArray(tokenIds)), _) => 
                    (tokenIds must contain(JString(tokenId))) && 
                    (jsonTestService.get[JValue]("/tokens/" + tokenId) must whenDelivered ({
                      beLike[HttpResponse[JValue]] {
                        case HttpResponse(status, _, Some(jtoken), _) => 
                          jtoken.validated[Token] must beLike {
                            case Success(token) => 
                              (token.permissions.read must beTrue) && 
                              (token.permissions.share must beFalse) &&
                              (token.tokenId must_== tokenId)
                          }
                      }
                    })(overrideFutureTimeouts))
                }
              })(overrideFutureTimeouts))
          }
        }
    }

    "create child tokens with a trailing slash" in {
        val newToken = Token.Test.issue(permissions = Permissions(read = true, write = true, share = false, explore = false))
        jsonTestService.post[JValue]("/tokens/")(newToken.serialize) must whenDelivered {
          beLike {
            case HttpResponse(HttpStatus(status, _), _, Some(JString(result)), _) => 
              (status must_== HttpStatusCodes.OK) && 
              (result.length must_== Token.Test.tokenId.length) &&
              (jsonTestService.get[JValue]("/tokens/") must whenDelivered {
                beLike {
                  case HttpResponse(status, _, Some(JArray(tokenIds)), _) => tokenIds must contain(JString(result))
                }
              })
          }
        }
    }

    "explore variables" in {
      //skip("disabled")
      (jsonTestService.get[JValue]("/vfs/test/.tweeted")) must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(result), _) => 
            val expected = List(".startup",".retweet",".otherStartups",".~tweet",".location",".twitterClient",".gender",".recipientCount")
            result.deserialize[List[String]] must haveTheSameElementsAs(expected)
        }
      } 
    }

    "count created events" in {
      //skip("disabled")
      lazy val tweetedCount = sampleEvents.count {
        case Event("tweeted", _, _) => true
        case _ => false
      }

      val queryTerms = JObject(
        JField("location", "usa") :: Nil
      )

      (jsonTestService.post[JValue]("/vfs/test/.tweeted/count")(queryTerms)) must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(result), _) => result.deserialize[Long] must_== tweetedCount
        }
      } 
    }

    "count events by get" in {
      //skip("disabled")
      lazy val tweetedCount = sampleEvents.count {
        case Event("tweeted", _, _) => true
        case _ => false
      }

      jsonTestService.get[JValue]("/vfs/test/.tweeted/count?location=usa") must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(result), _) => result.deserialize[Long] must_== tweetedCount
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

    "return variable series means" in {
      //skip("disabled")
      val (events, minDate, maxDate) = timeSlice(sampleEvents, Hour)
      val expected = expectedMeans(events, "recipientCount", keysf(Hour))

      val queryTerms = JObject(
        JField("start", minDate.getMillis) ::
        JField("end", maxDate.getMillis) ::
        JField("location", "usa") :: Nil
      )

      (jsonTestService.post[JValue]("/vfs/test/.tweeted.recipientCount/series/hour/means")(queryTerms)) must whenDelivered {
        verify {
          case HttpResponse(status, _, Some(contents), _) => 
            val resultData = contents match {
              case JArray(values) => values.flatMap { 
                case JArray(List(JObject(List(JField("timestamp", k), JField("location", k2))), JDouble(v))) => Some((List(k.deserialize[Instant].toString, k2.deserialize[String]), v))
                case JArray(List(JObject(List(_, _)), JNull)) => None
              }
            }

            resultData.toMap must haveTheSameElementsAs(expected("tweeted"))
        }
      } 
    }

    "return variable value series counts" in {
      //skip("disabled")
      val granularity = Hour
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)
      val expectedTotals = valueCounts(events)

      val queryTerms = JObject(
        JField("start", minDate.getMillis) ::
        JField("end", maxDate.getMillis) ::
        JField("location", "usa") :: Nil
      )

      val ((jpath, value), count) = (expectedTotals.find{ case ((k, _), _) => k.nodes.last == JPathField("gender") }).get

      val vtext = compact(render(value))
      val servicePath = "/vfs/test/"+jpath+"/values/"+vtext+"/series/hour"
      if (!jpath.endsInInfiniteValueSpace) {
        (jsonTestService.post[JValue](servicePath)(queryTerms)) must whenDelivered {
          verify {
            case HttpResponse(status, _, Some(JArray(values)), _) => (values must notBeEmpty) //&& (series must_== expected)
          }
        }
      }
    }

    "group variable value series counts" in {
      //skip("disabled")
      val granularity = Hour
      val (events, minDate, maxDate) = timeSlice(sampleEvents, granularity)
      val expectedTotals = valueCounts(events)

      val queryTerms = JObject(
        JField("start", minDate.getMillis) ::
        JField("end", maxDate.getMillis) ::
        JField("location", "usa") :: Nil
      )

      val ((jpath, value), count) = (expectedTotals.find{ case ((k, _), _) => k.nodes.last == JPathField("gender") }).get

      val vtext = compact(render(value))
      val servicePath = "/vfs/test/"+jpath+"/values/"+vtext+"/series/hour?groupBy=day"
      if (!jpath.endsInInfiniteValueSpace) {
        (jsonTestService.post[JValue](servicePath)(queryTerms)) must whenDelivered {
          verify {
            case HttpResponse(status, _, Some(JArray(values)), _) => (values must notBeEmpty) //&& (series must_== expected)
          }
        }
      }
    }

    "grouping in intersection queries" >> {
      "timezone shifting must not discard data" in {
        //skip("disabled")
        //pendingUntilFixed {
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
        //}
      }
    }
  }
}

class RootTrackingServiceSpec extends TestAnalyticsService with ArbitraryEvent with FutureMatchers with PendingUntilFixed {
  override val genTimeClock = PastClock(Days.TWO.toStandardDuration)

  "When writing to the service root" should {
    shareVariables()

    val sampleEvents: List[Event] = containerOfN[List, Event](10, fullEventGen).sample.get ->- {
      _.foreach(event => jsonTestService.post[JValue]("/vfs/")(event.message))
    }

    "count events by get" in {
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
          case HttpResponse(status, _, Some(JArray(elements)), _) => 
            (elements map { case JString(s) => s }) must contain(".tweeted")
          case x => false
        }
      } 
    }

    "retrieve property children at the root" in {
      jsonTestService.get[JValue]("/vfs/.tweeted?location=usa") must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(JArray(elements)), _) => (elements map { case JString(s) => s }) must contain(".twitterClient")
        }
      } 
    }
  }
}

class RollupAnalyticsServiceSpec extends TestAnalyticsService with ArbitraryEvent with FutureMatchers with PendingUntilFixed {
  override val genTimeClock = clock 

  "Analytics Service" should {
    shareVariables()

    val sampleEvents: List[Event] = containerOfN[List, Event](10, fullEventGen).sample.get ->- {
      _.foreach(event => jsonTestService.query("rollup", "true").post[JValue]("/vfs/test")(event.message))
    }

    "roll up data to parent paths" in {
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


class ArchivalAnalyticsServiceSpec extends TestAnalyticsService with ArbitraryEvent with FutureMatchers with PendingUntilFixed {
  override val genTimeClock = PastClock(Days.TWO.toStandardDuration)

  "Analytics Service" should {
    shareVariables()

    val sampleEvents: List[Event] = containerOfN[List, Event](10, fullEventGen).sample.get ->- {
      _.foreach(event => jsonTestService.post[JValue]("/vfs/test")(event.message))
    }

    "store events in the events database, but not in the index." in {
      val (beforeCutoff, afterCutoff) = sampleEvents.partition(_.timestamp.exists(_ <= clock.now.minusDays(1)))

      lazy val tweetedCount = afterCutoff.count {
        case Event("tweeted", _, _) => true
        case _ => false
      }

      beforeCutoff must notBeEmpty
      afterCutoff must notBeEmpty
      jsonTestService.get[JValue]("/vfs/test/.tweeted/count?location=usa") must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(result), _) => result.deserialize[Long] must_== tweetedCount
        }
      } 
    }
  }
}

