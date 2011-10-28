package com.reportgrid.analytics

import org.specs.Specification

import blueeyes.concurrent.Future
import blueeyes.concurrent.test.FutureMatchers
import blueeyes.json._
import blueeyes.json.JsonAST._
import org.joda.time.Instant

class TagsSpec extends Specification with FutureMatchers {
  import Tag._
  "Tag.extractTags" should {
    "correctly return identified tags" in {
      val extractor = Tag.timeTagExtractor(TimeSeriesEncoding.Default, new Instant, true)
      val obj = JsonParser.parse("""{ "type" : "click", "#timestamp": 1315368953984 }""") --> classOf[JObject]

      val (tags, remainder) = extractTags(List(extractor), obj)
      remainder must_== JsonParser.parse("""{ "type" : "click"}""")
      tags must beLike {
        case Tags(tags) => tags must whenDelivered {
          beEqualTo {
            Tag("timestamp", TimeReference(TimeSeriesEncoding.Default, new Instant(1315368953984L))) :: Nil
          }
        }
      }
    }

    "correctly handle the skipping of tags" in {
      val extractors = Tag.timeTagExtractor(TimeSeriesEncoding.Default, new Instant, true) ::
                       Tag.locationTagExtractor(Future.sync(None)) :: Nil
                        
      val obj = JsonParser.parse("""{"type" : "click", "#timestamp": 1315368953984 }""") --> classOf[JObject]

      val (tags, remainder) = extractTags(extractors, obj)
      remainder must_== JsonParser.parse("""{"type" : "click"}""")
      tags must beLike {
        case Tags(tags) => tags must whenDelivered {
          beEqualTo {
            Tag("timestamp", TimeReference(TimeSeriesEncoding.Default, new Instant(1315368953984L))) :: Nil
          }
        }
      }
    }

      // This test is currently disabled becaus handling malformed tags is something that we probably don't actually want to do
//    "correctly handle malformed location tags" in {
//      val extractors = Tag.timeTagExtractor(TimeSeriesEncoding.Default, new Instant, true) ::
//                       Tag.locationTagExtractor(Future.sync(None)) :: Nil
//                        
//      val obj = JsonParser.parse("""{"type": "click", "#location": {"country":"Brazil","region":"Brazil/(null)","city":"Brazil/(null)/"}}""") --> classOf[JObject]
//
//      val (tags, remainder) = extractTags(extractors, obj)
//      remainder must_== JsonParser.parse("""{"type" : "click"}""")
//      tags must beLike {
//        case Tags(tags) => tags must whenDelivered {
//          beEqualTo {
//            Hierarchy.of(Hierarchy.NamedLocation("country", "Brazil") :: Hierarchy.NamedLocation("region", "(null)") :: Nil).map(Tag("location", _)).toOption.get
//          }
//        }
//      }
//    }
  }

  "Tag.timeTagExtractor" should {
    "return an extractor that correctly parses a timestamp" in {
      val extractor = Tag.timeTagExtractor(TimeSeriesEncoding.Default, new Instant, true)

      val obj = JsonParser.parse("""{ "type" : "click", "#timestamp": 1315368953984 }""") --> classOf[JObject]

      val (tags, remainder) = extractor(obj) 
      remainder must_== JsonParser.parse("""{ "type" : "click"}""")
      tags must beLike {
        case Tags(tags) => tags must whenDelivered {
          beEqualTo {
            Tag("timestamp", TimeReference(TimeSeriesEncoding.Default, new Instant(1315368953984L))) :: Nil
          }
        }
      }
    }
  }

  "Hiearchy.respectsRefinementRule" should {
    "accept a valid simple hierarchy" in {
      val valid = List(
        com.reportgrid.analytics.Path("/foo"),
        com.reportgrid.analytics.Path("/foo/bar"),
        com.reportgrid.analytics.Path("/foo/bar/baz")
      )

      Hierarchy.respectsRefinementRule(valid) must beTrue
    }

    "accept a valid hierarchy with gaps" in {
      val valid = List(
        com.reportgrid.analytics.Path("/foo"),
        com.reportgrid.analytics.Path("/foo/bar"),
        com.reportgrid.analytics.Path("/foo/bar/baz/gweep")
      )

      Hierarchy.respectsRefinementRule(valid) must beTrue
    }

    "detect refinement rule violations" in {
      val invalid = List(
        com.reportgrid.analytics.Path("/foo"),
        com.reportgrid.analytics.Path("/foo/bar"),
        com.reportgrid.analytics.Path("/foo/baz")
      )

      Hierarchy.respectsRefinementRule(invalid) must beFalse
    }
  }
}

// vim: set ts=4 sw=4 et:
