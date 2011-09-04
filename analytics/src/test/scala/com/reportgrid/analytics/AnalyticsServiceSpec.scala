package com.reportgrid.analytics

import blueeyes._
import blueeyes.core.data.Bijection.identity
import blueeyes.core.http.{HttpStatus, HttpResponse, MimeTypes}
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.concurrent.test._
import blueeyes.util.metrics.Duration._

import MimeTypes._

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL
import blueeyes.json.JsonDSL._
import blueeyes.json.xschema.JodaSerializationImplicits._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.JPathImplicits._
import blueeyes.persistence.mongo.{Mongo, RealMongo, MockMongo}

import com.reportgrid.api.ReportGridTrackingClient

import java.util.Date
import net.lag.configgy.{Configgy, ConfigMap}

import org.specs._
import org.scalacheck.Gen._
import scalaz.Scalaz._

import rosetta.json.blueeyes._

import Periodicity._
import persistence.MongoSupport._
import org.joda.time.Instant

trait TestAnalyticsService extends BlueEyesServiceSpecification with AnalyticsService with LocalMongo {
  override val configuration = "services{analytics{v0{" + mongoConfigFileData + "}}}"

  //override def mongoFactory(config: ConfigMap): Mongo = new RealMongo(config)
  override def mongoFactory(config: ConfigMap): Mongo = new MockMongo()

  def auditClient(config: ConfigMap) = external.NoopTrackingClient
  def yggdrasil(configMap: ConfigMap) = external.Yggdrasil.Noop[JValue]
  def jessup(configMap: ConfigMap) = external.Jessup.Noop

  lazy val jsonTestService = service.contentType[JValue](application/(MimeTypes.json)).
                                     query("tokenId", Token.Test.tokenId)

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(40, 1000L.milliseconds)
}

class AnalyticsServiceSpec extends TestAnalyticsService with ArbitraryEvent with FutureMatchers {
  "Analytics Service" should {
    shareVariables()

    val sampleEvents: List[Event] = containerOfN[List, Event](10, eventGen).sample.get ->- {
      _.foreach(event => jsonTestService.post[JValue]("/vfs/test")(event.message))
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
  }
}

class AnalyticsServiceFailingCountSpec extends TestAnalyticsService with FutureMatchers {
  val eventsFailingCount = List(
    """{
      "events":{
        "tweeted":{
          "location":"location7",
          "retweet":false,
          "gender":"female",
          "recipientCount":2,
          "startup":"startup38",
          "otherStartups":["startup49"],
          "twitterClient":"client0",
          "~tweet":"ei98xehylurxikzE7Cnmwzrs"
        }
      },
      "#timestamp":1314868551480,
      "#location":{
        "country":"/usa/",
        "state":"/usa/colorado/",
        "city":"/usa/colorado/lakewood/",
        "zip":"/usa/colorado/lakewood/80215/"
      }
    }""",
    """{
      "events":{
        "funded":{
          "location":"location6",
          "retweet":false,
          "gender":"male",
          "recipientCount":3,
          "startup":"startup5",
          "otherStartups":[],
          "twitterClient":"client7",
          "~tweet":"gfpivvuhyidgpuj8tzhgprbt0u0Hjkmuhb7oFvfqjdvb2dhzx0ggoqb4VUwxvguex6xlfybk0UjjnExtjviRxkzlpg"
        }
      },
      "#timestamp":1314856934791,
      "#location":{
        "country":"/usa/",
        "state":"/usa/colorado/",
        "city":"/usa/colorado/boulder/",
        "zip":"/usa/colorado/boulder/80304/"
      }
    }""",
    """{
      "events":{
        "tweeted":{
          "location":"location10",
          "retweet":false,
          "gender":"male",
          "recipientCount":3,
          "startup":"startup31",
          "otherStartups":[],
          "twitterClient":"client2",
          "~tweet":"vz8mvfyjbnuaxfykkrGrp"
        }
      },
      "#timestamp":1314841151841,
      "#location":{
        "country":"/usa/",
        "state":"/usa/colorado/",
        "city":"/usa/colorado/boulder/",
        "zip":"/usa/colorado/boulder/80304/"
      }
    }""",
    """{
      "events":{
        "tweeted":{
          "location":"location10",
          "retweet":false,
          "gender":"female",
          "recipientCount":1,
          "startup":"startup17",
          "otherStartups":[],
          "twitterClient":"client0",
          "~tweet":"ujowfhkqefo9esuj3Uvmtua5gv9qdK6hbnOl"
        }
      },
      "#timestamp":1314846519413,
      "#location":{
        "country":"/usa/",
        "state":"/usa/colorado/",
        "city":"/usa/colorado/boulder/",
        "zip":"/usa/colorado/boulder/80304/"
      }
    }""",
    """{
      "events":{
        "funded":{
          "location":"location1",
          "retweet":true,
          "gender":"male",
          "recipientCount":3,
          "startup":"startup50",
          "otherStartups":[],
          "twitterClient":"client1",
          "~tweet":"rhnaivtfosxu9jKmqgwuugfk0fumjybglzzlQnfaqczlcqskf7fkthIygj01t"
        }
      },
      "#timestamp":1314866913425,
      "#location":{
        "country":"/usa/",
        "state":"/usa/colorado/",
        "city":"/usa/colorado/boulder/",
        "zip":"/usa/colorado/boulder/80305/"
      }
    }""",
    """{
      "events":{
        "funded":{
          "location":"location3",
          "retweet":true,
          "gender":"female",
          "recipientCount":3,
          "startup":"startup23",
          "otherStartups":[],
          "twitterClient":"client0",
          "~tweet":"ppawI3Ylpxqfsfd0nxZhsqniefz0qqd105sitlbpkjwdb"
        }
      },
      "#timestamp":1314909234053,
      "#location":{
        "country":"/usa/",
        "state":"/usa/colorado/",
        "city":"/usa/colorado/boulder/",
        "zip":"/usa/colorado/boulder/80305/"
      }
    }""",
    """{
      "events":{
        "funded":{
          "location":"location5",
          "retweet":true,
          "gender":"female",
          "recipientCount":3,
          "startup":"startup0",
          "otherStartups":[],
          "twitterClient":"client1",
          "~tweet":"afhw2vqp5wdgkzloahva6zjJvgBoVslrqhKrcqqjkxrxaa1drmdaticoufwanpikehcmlasgpvhVu3oqms0kht9gtgi9zkhppudme"
        }
      },
      "#timestamp":1314881950324,
      "#location":{
        "country":"/usa/",
        "state":"/usa/colorado/",
        "city":"/usa/colorado/lakewood/",
        "zip":"/usa/colorado/lakewood/80215/"
      }
    }""",
    """{
      "events":{
        "tweeted":{
          "location":"location1",
          "retweet":false,
          "gender":"male",
          "recipientCount":2,
          "startup":"startup18",
          "otherStartups":["startup42","startup2"],
          "twitterClient":"client1",
          "~tweet":"vldIiwmgbroteswscqLlzfkfqVgumlZkkhrpBLqghers07ma0ybyxwGfgUisdvdLw9zv1ibzbEcz8ja7j"
        }
      },
      "#timestamp":1314839519918,
      "#location":{
        "country":"/usa/",
        "state":"/usa/arizona/",
        "city":"/usa/arizona/tucson/",
        "zip":"/usa/arizona/tucson/85716/"
      }
    }""",
    """{
      "events":{
        "funded":{
          "location":"location0",
          "retweet":false,
          "gender":"female",
          "recipientCount":2,
          "startup":"startup23",
          "otherStartups":[],
          "twitterClient":"client0",
          "~tweet":"tuxsa0jZoovlm7rGzsr5lmy"
        }
      },
      "#timestamp":1314904370061,
      "#location":{
        "country":"/usa/",
        "state":"/usa/arizona/",
        "city":"/usa/arizona/tucson/",
        "zip":"/usa/arizona/tucson/85716/"
      }
    }""",
    """{
      "events":{
        "funded":{
          "location":"location3",
          "retweet":false,
          "gender":"female",
          "recipientCount":0,
          "startup":"startup1",
          "otherStartups":[],
          "twitterClient":"client6",
          "~tweet":"iuhfwmoecgnaxlfukwzm0Ic2e98iGJuuwdm3kxh9lR2afpnv3yieybLs1dzu7dgowuiLlk2nnfhutgpcfai5lglsdxtn6mbj"
        }
      },
      "#timestamp":1314891769229,
      "#location":{
        "country":"/usa/",
        "state":"/usa/colorado/",
        "city":"/usa/colorado/lakewood/",
        "zip":"/usa/colorado/lakewood/80215/"
      }
    }"""
  )

  val queryUrl = "/vfs/test/.funded.gender/values/\"male\"/series/hour"

  val queryContent = """{
    "start":1314846000000,
    "end":1314892800000,
    "location":"usa"
  }"""


  "counting variable values" should {
    shareVariables()
    eventsFailingCount.foreach(event => jsonTestService.post[JValue]("/vfs/test")(JsonParser.parse(event)))

    "return variable value series counts" in {
      (jsonTestService.post[JValue](queryUrl)(JsonParser.parse(queryContent))) must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(JArray(values)), _) => 
            values must notBeEmpty
        }
      }
    }
  }
}

class AnalyticsServicePassingCountSpec extends TestAnalyticsService with FutureMatchers {
  val eventsPassingCount = List(
    """{
      "events":{
        "tweeted":{
          "location":"location8",
          "retweet":true,
          "gender":"female",
          "recipientCount":2,
          "startup":"startup0",
          "otherStartups":["startup7"],
          "twitterClient":"client7",
          "~tweet":"joRgufzqJoBorbmlubEe23sWasVnxjylHala2k3dbcv0Lhs203nox1il8cinblkHkgw7kkr4yjkbxkkslbKzjdbdXtgnitlt"
        }
      },
      "#timestamp":1314867825488,
      "#location":{
        "country":"/usa/",
        "state":"/usa/colorado/",
        "city":"/usa/colorado/lakewood/",
        "zip":"/usa/colorado/lakewood/80215/"
      }
    }""",
    """{
      "events":{
        "tweeted":{
          "location":"location1",
          "retweet":false,
          "gender":"male",
          "recipientCount":2,
          "startup":"startup26",
          "otherStartups":["startup43","startup45"],
          "twitterClient":"client5",
          "~tweet":"qcynmtoloaAnbs7IfvRtmv5gaghjdqrcrAfrtfwschhmlsmrguzreaywqdefdysgwP5ktseI5f"
        }
      },
      "#timestamp":1314885028830,
      "#location":{
        "country":"/usa/",
        "state":"/usa/arizona/",
        "city":"/usa/arizona/tucson/",
        "zip":"/usa/arizona/tucson/85716/"
      }
    }""",
    """{
      "events":{
        "funded":{
          "location":"location8",
          "retweet":false,
          "gender":"female",
          "recipientCount":3,
          "startup":"startup39",
          "otherStartups":["startup14"],
          "twitterClient":"client0",
          "~tweet":"itjKsoWmghakmes0womaejRkdrj0r9oumLiuun7lqkqpj7lwPsxb2vuvircbxjzWBarhqFr86qDa1ERbaq4wcy8gszfgvujtXYc"
        }
      },
      "#timestamp":1314862176112,
      "#location":{
        "country":"/usa/",
        "state":"/usa/arizona/",
        "city":"/usa/arizona/tucson/",
        "zip":"/usa/arizona/tucson/85716/"
      }
    }""",
    """{
      "events":{
        "funded":{
          "location":"location6",
          "retweet":true,
          "gender":"female",
          "recipientCount":2,
          "startup":"startup9",
          "otherStartups":[],
          "twitterClient":"client10",
          "~tweet":"ii6rjipkkmqceivmqotajpd"
        }
      },
      "#timestamp":1314871330489,
      "#location":{
        "country":"/usa/",
        "state":"/usa/colorado/",
        "city":"/usa/colorado/lakewood/",
        "zip":"/usa/colorado/lakewood/80215/"
      }
    }""",
    """{
      "events":{
        "tweeted":{
          "location":"location2",
          "retweet":true,
          "gender":"male",
          "recipientCount":3,
          "startup":"startup43",
          "otherStartups":[],
          "twitterClient":"client10",
          "~tweet":"i7zfjqntpatnyu6h2vsAeqcm93d6VhuaTjMedrpKcrhjxparhw1kxuyu7Ymd6lwgdmzQejfr4boksefy"
        }
      },
      "#timestamp":1314917350638,
      "#location":{
        "country":"/usa/",
        "state":"/usa/arizona/",
        "city":"/usa/arizona/tucson/",
        "zip":"/usa/arizona/tucson/85716/"
      }
    }""",
    """{
      "events":{
        "tweeted":{
          "location":"location8",
          "retweet":false,
          "gender":"male",
          "recipientCount":0,
          "startup":"startup10",
          "otherStartups":[],
          "twitterClient":"client7",
          "~tweet":"pmyr41sypenRxsqnbQhbienwvejGgfgzocgRajqxfmycyfsgdDaoxahsbnu5aifhw0ipgn"
        }
      },
      "#timestamp":1314915680710,
      "#location":{
        "country":"/usa/",
        "state":"/usa/colorado/",
        "city":"/usa/colorado/lakewood/",
        "zip":"/usa/colorado/lakewood/80215/"
      }
    }""",
    """{
      "events":{
        "funded":{
          "location":"location8",
          "retweet":true,
          "gender":"female",
          "recipientCount":1,
          "startup":"startup43",
          "otherStartups":[],
          "twitterClient":"client3",
          "~tweet":"sszeo5qhxkOloxjrsiuujwzDqhrdFnikpnthdnpsxq3cwnl6derbvhmpezleenhitgyvz5Coswg"
        }
      },
      "#timestamp":1314887621580,
      "#location":{
        "country":"/usa/",
        "state":"/usa/arizona/",
        "city":"/usa/arizona/tucson/",
        "zip":"/usa/arizona/tucson/85716/"
      }
    }""",
    """{
      "events":{
        "tweeted":{
          "location":"location8",
          "retweet":false,
          "gender":"male",
          "recipientCount":2,
          "startup":"startup35",
          "otherStartups":[],
          "twitterClient":"client7",
          "~tweet":"vwbal82t4e5fh6naqlgqrwh6RrdvqagR1rdehrotimaohw9drf0jjuJjzbfzq"
        }
      },
      "#timestamp":1314922912654,
      "#location":{
        "country":"/usa/",
        "state":"/usa/colorado/",
        "city":"/usa/colorado/lakewood/",
        "zip":"/usa/colorado/lakewood/80215/"
      }
    }""",
    """{
      "events":{
        "tweeted":{
          "location":"location7",
          "retweet":true,
          "gender":"female",
          "recipientCount":1,
          "startup":"startup11",
          "otherStartups":[],
          "twitterClient":"client5",
          "~tweet":"ysbqklimgcozwfvqdbiuvun8ctnh2vwsxUsmxbajoZkngitPbr3exuyfias5htatGssepgnxkS5f1vbVqiudeivzjthi7b"
        }
      },
      "#timestamp":1314901245114,
      "#location":{
        "country":"/usa/",
        "state":"/usa/arizona/",
        "city":"/usa/arizona/tucson/",
        "zip":"/usa/arizona/tucson/85716/"
      }
    }""",
    """{
      "events":{
        "tweeted":{
          "location":"location9",
          "retweet":false,
          "gender":"male",
          "recipientCount":3,
          "startup":"startup48",
          "otherStartups":[],
          "twitterClient":"client3",
          "~tweet":"plkFlhfvxzjnagozxb5etmmjwhm9tC2dwlysc5"
        }
      },
      "#timestamp":1314902539145,
      "#location":{
        "country":"/usa/",
        "state":"/usa/arizona/",
        "city":"/usa/arizona/tucson/",
        "zip":"/usa/arizona/tucson/85716/"
      }
    }"""
  )

  val queryContent = """{
    "start":1314871200000,
    "end":1314918000000,
    "location":"usa"
  }"""

  val queryUrl = "/vfs/test/.tweeted.gender/values/\"male\"/series/hour"

  "counting variable values" should {
    shareVariables()
    eventsPassingCount.foreach(event => jsonTestService.post[JValue]("/vfs/test")(JsonParser.parse(event)))

    "return variable value series counts" in {
      (jsonTestService.post[JValue](queryUrl)(JsonParser.parse(queryContent))) must whenDelivered {
        beLike {
          case HttpResponse(status, _, Some(JArray(values)), _) => 
            values must notBeEmpty
        }
      }
    }
  }
}
