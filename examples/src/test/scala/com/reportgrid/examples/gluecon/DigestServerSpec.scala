package com.reportgrid.examples.gluecon

import org.codehaus.jackson._
import org.codehaus.jackson.map._
import blueeyes.json.JsonAST._

import org.specs._
import GlueConGnipDigester._

class DigestServerSpec extends Specification {
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
    JField("languages",JArray(List(JString("en")))), 
    JField("clout",JInt(16)), 
    JField("friendsCount",JInt(240)), 
    JField("followersCount",JInt(400)), 
    JField("startups",JArray(List(JString("ReportGrid"))))
  )

  val expectedFields2 = List(
    JField("languages",JArray(List(JString("en")))), 
    JField("clout",JInt(11)), 
    JField("friendsCount",JInt(2000)), 
    JField("followersCount",JInt(2200)), 
    JField("startups",JArray(List(JString("ReportGrid")))), 
    JField("client",JString("web"))
  )
  
  "Processing an entry from the Gnip json stream of tweets" should {
    "correctly identify property information" in {
      val jsonFactory = (new ObjectMapper).getJsonFactory()
      val parser = jsonFactory.createJsonParser(sampleData)
      parser.nextToken match {
        case JsonToken.START_OBJECT => 
          val Tweet(startups, properties, time) = extractTweet(parser) 
          startups mustNot beEmpty
          properties must beLike {
            case JObject(fields) => fields must haveTheSameElementsAs(expectedFields1)
          }
        
        case other => fail("Got unexpected JSON token: " + other)
      }
    }

    "parse a stream" in {
      val jsonFactory = (new ObjectMapper).getJsonFactory()
      val parser = jsonFactory.createJsonParser(sampleData)

      val tweets = parse(parser).toList

      tweets must haveSize(2)
      List(expectedFields1, expectedFields2).forall {
        fields => tweets must exist {
          case Tweet(_, properties, _) => properties must beLike {
            case JObject(fields) => fields must haveTheSameElementsAs(fields)
          }
        }
      }
    }
  }
}


// vim: set ts=4 sw=4 et:
