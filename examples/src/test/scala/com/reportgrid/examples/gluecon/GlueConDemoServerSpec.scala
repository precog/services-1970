package com.reportgrid.examples.gluecon

import org.codehaus.jackson._
import org.codehaus.jackson.map._
import blueeyes.json.JsonAST._
import net.lag.configgy.Configgy
import net.lag.configgy.Config

import com.reportgrid.api._
import com.reportgrid.api.blueeyes.ReportGrid

import org.specs._

class GlueConDemoServerSpec extends Specification {
  val sampleData = """
    {
        "id": "tag:search.twitter.com,2005:72418830005182464",
        "body": "Going all-out getting ready to show off @ReportGrid's stuff at GlueCon next Weds/Thurs. Come and see what realtime analytics should be like!",
        "verb": "post",
        "link": "http://twitter.com/nuttycom/statuses/72418830005182464",
        "generator": {
            "link": "http://twitter.com",
            "displayName": "web"
        },
        "postedTime": "2011-05-22T21:49:29.000Z",
        "provider": {
            "link": "http://www.twitter.com",
            "displayName": "Twitter",
            "objectType": "service"
        },
        "object": {
            "summary": "Going all-out getting ready to show off @ReportGrid's stuff at GlueCon next Weds/Thurs. Come and see what realtime analytics should be like!",
            "id": "object:search.twitter.com,2005:72418830005182464",
            "link": "http://twitter.com/nuttycom/statuses/72418830005182464",
            "postedTime": "2011-05-22T21:49:29.000Z",
            "objectType": "note"
        },
        "actor": {
            "summary": "Scala coder, climber, go player",
            "friendsCount": 249,
            "location": {
                "displayName": "Boulder, CO",
                "objectType": "place"
            },
            "link": "http://www.twitter.com/nuttycom",
            "postedTime": "2008-09-24T16:18:32.000Z",
            "image": "http://a3.twimg.com/profile_images/956676924/195ea48e-6a4d-456a-acf3-0725f5727a22_normal.jpg",
            "links": [
                {
                    "rel": "me",
                    "href": "http://logji.blogspot.com"
                }
            ],
            "listedCount": 57,
            "id": "id:twitter.com:16436452",
            "languages": [
                "en"
            ],
            "followersCount": 451,
            "utcOffset": "-25200",
            "preferredUsername": "nuttycom",
            "displayName": "Kris Nuttycombe",
            "statusesCount": 2938,
            "objectType": "person"
        },
        "twitter_entities": {
            "user_mentions": [
                {
                    "indices": [
                        40,
                        51
                    ],
                    "screen_name": "ReportGrid",
                    "id_str": "203594440",
                    "name": "ReportGrid",
                    "id": 203594440
                }
            ],
            "hashtags": [],
            "urls": []
        },
        "objectType": "activity",
        "gnip": {
            "language": {
                "value": "en"
            },
            "matching_rules": [
                {
                    "value": "ReportGrid",
                    "tag": null
                }
            ]
        }
    }
    {
        "id": "tag:search.twitter.com,2005:72424202015215617",
        "body": "RT @nuttycom: Going all-out getting ready to show off @ReportGrid's stuff at GlueCon next Weds/Thurs. Come and see what realtime analyti ...",
        "verb": "share",
        "link": "http://twitter.com/defrag/statuses/72424202015215617",
        "generator": {
            "link": "http://twitter.com",
            "displayName": "web"
        },
        "postedTime": "2011-05-22T22:10:49.000Z",
        "provider": {
            "link": "http://www.twitter.com",
            "displayName": "Twitter",
            "objectType": "service"
        },
        "object": {
            "id": "tag:search.twitter.com,2005:72418830005182464",
            "body": "Going all-out getting ready to show off @ReportGrid's stuff at GlueCon next Weds/Thurs. Come and see what realtime analytics should be like!",
            "verb": "post",
            "link": "http://twitter.com/nuttycom/statuses/72418830005182464",
            "generator": {
                "link": "http://twitter.com",
                "displayName": "web"
            },
            "postedTime": "2011-05-22T21:49:29.000Z",
            "provider": {
                "link": "http://www.twitter.com",
                "displayName": "Twitter",
                "objectType": "service"
            },
            "object": {
                "summary": "Going all-out getting ready to show off @ReportGrid's stuff at GlueCon next Weds/Thurs. Come and see what realtime analytics should be like!",
                "id": "object:search.twitter.com,2005:72418830005182464",
                "link": "http://twitter.com/nuttycom/statuses/72418830005182464",
                "postedTime": "2011-05-22T21:49:29.000Z",
                "objectType": "note"
            },
            "actor": {
                "summary": "Scala coder, climber, go player",
                "friendsCount": 249,
                "location": {
                    "displayName": "Boulder, CO",
                    "objectType": "place"
                },
                "link": "http://www.twitter.com/nuttycom",
                "postedTime": "2008-09-24T16:18:32.000Z",
                "image": "http://a3.twimg.com/profile_images/956676924/195ea48e-6a4d-456a-acf3-0725f5727a22_normal.jpg",
                "links": [
                    {
                        "rel": "me",
                        "href": "http://logji.blogspot.com"
                    }
                ],
                "listedCount": 57,
                "id": "id:twitter.com:16436452",
                "languages": [
                    "en"
                ],
                "followersCount": 451,
                "utcOffset": "-25200",
                "preferredUsername": "nuttycom",
                "displayName": "Kris Nuttycombe",
                "statusesCount": 2938,
                "objectType": "person"
            },
            "twitter_entities": {
                "user_mentions": [
                    {
                        "indices": [
                            40,
                            51
                        ],
                        "screen_name": "ReportGrid",
                        "id_str": "203594440",
                        "name": "ReportGrid",
                        "id": 203594440
                    }
                ],
                "urls": [],
                "hashtags": []
            },
            "objectType": "activity"
        },
        "actor": {
            "summary": "tech conf producer",
            "friendsCount": 2066,
            "location": {
                "displayName": "FLA; confs - Denver",
                "objectType": "place"
            },
            "link": "http://www.twitter.com/defrag",
            "postedTime": "2007-09-12T01:34:45.000Z",
            "image": "http://a2.twimg.com/profile_images/203211197/glueanddefrag.pdf__1_page__normal.jpg",
            "links": [
                {
                    "rel": "me",
                    "href": "http://www.defragcon.com"
                }
            ],
            "listedCount": 134,
            "id": "id:twitter.com:8823362",
            "languages": [
                "en"
            ],
            "followersCount": 2227,
            "utcOffset": "-18000",
            "preferredUsername": "defrag",
            "displayName": "Defrag/Glue",
            "statusesCount": 15370,
            "objectType": "person"
        },
        "twitter_entities": {
            "user_mentions": [
                {
                    "indices": [
                        3,
                        12
                    ],
                    "screen_name": "nuttycom",
                    "id_str": "16436452",
                    "name": "Kris Nuttycombe",
                    "id": 16436452
                },
                {
                    "indices": [
                        54,
                        65
                    ],
                    "screen_name": "ReportGrid",
                    "id_str": "203594440",
                    "name": "ReportGrid",
                    "id": 203594440
                }
            ],
            "urls": [],
            "hashtags": []
        },
        "objectType": "activity",
        "gnip": {
            "language": {
                "value": "en"
            },
            "matching_rules": [
                {
                    "value": "ReportGrid",
                    "tag": null
                }
            ]
        }
    }
  """

  val expectedFields1 = List(
    JField("client",JString("web")),
    JField("emotion",JString("excited")),
    JField("languages",JArray(List(JString("en")))), 
    JField("clout",JInt(16)), 
    JField("friendsCount",JInt(240)), 
    JField("followersCount",JInt(400)), 
    JField("startup",JString("ReportGrid"))
  )

  val expectedFields2 = List(
    JField("languages",JArray(List(JString("en")))), 
    JField("clout",JInt(11)), 
    JField("friendsCount",JInt(2000)), 
    JField("followersCount",JInt(2200)), 
    JField("startup",JString("ReportGrid")), 
    JField("client",JString("web"))
  )

  val digester = new GlueConGnipDigester(new ReportGrid("", ReportGridConfig.Local), new GlueConCompanies(None))
  
  "Processing an entry from the Gnip json stream of tweets" should {
    "correctly identify property information" in {
      val jsonFactory = (new ObjectMapper).getJsonFactory()
      val parser = jsonFactory.createJsonParser(sampleData)
      parser.nextToken match {
        case JsonToken.START_OBJECT => 
          val List(Mention(_, properties, _)) = digester.extractMentions(parser) 
          properties must beLike {
            case JObject(fields) => fields must haveTheSameElementsAs(expectedFields1)
          }
        
        case other => fail("Got unexpected JSON token: " + other)
      }
    }

    "parse a stream" in {
      val jsonFactory = (new ObjectMapper).getJsonFactory()
      val parser = jsonFactory.createJsonParser(sampleData)

      val tweets = digester.parse(parser).take(2).toList

      List(expectedFields1, expectedFields2).forall {
        fields => tweets must exist {
          case List(Mention(_, properties, _)) => properties must beLike {
            case JObject(fields) => fields must haveTheSameElementsAs(fields)
          }

          case _ => fail("Shouldn't see a none.")
        }
      }
    }

    "detect an emotion" in {
      import GlueConGnipDigester._
      detectEmotion(""" Hi I'm really happy today :) """.split("""\s+""").toList) must beSome("happy")
      detectEmotion(""" Hi I'm really happy today :) but a little sad too. :-( """.split("""\s+""").toList) must beSome("happy")
    }
  }

//  "uploading rules to Gnip" should {
//    "update the rules" in {
//      val conf = """
//        gnipHost = "reportgrid-powertrack.gnip.com"
//        gnipPath = "data_collectors/1"
//        username = "reportgrid"
//        password = "trippyvizy"
//        tokenId = "A3BC1539-E8A9-4207-BB41-3036EC2C6E6D"
//      """
//
//      Configgy.configureFromString(conf)
//      GlueConDemoServer.loadRules(Configgy.config, new GlueConCompanies(None))
//    }
//  }

//  "connecting to the Gnip service" should {
//    "correctly handle the stream" in {
//      val conf = """
//        gnipHost = "reportgrid-powertrack.gnip.com"
//        gnipPath = "data_collectors/1/track.json"
//        username = "reportgrid"
//        password = "trippyvizy"
//        reportGridURL = "http://api.reportgrid.com/services/analytics/v0/"
//        tokenId = "A3BC1539-E8A9-4207-BB41-3036EC2C6E6D"
//      """
//
//      Configgy.configureFromString(conf)
//      GlueConDemoServer.run(Configgy.config, new GlueConCompanies(None))
//    }
//  }
}


// vim: set ts=4 sw=4 et:
