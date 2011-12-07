package com.reportgrid.analytics

import blueeyes._
import blueeyes.util._
import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.JodaSerializationImplicits._

import org.joda.time.Instant
import org.scalacheck.{Gen, Arbitrary}
import Gen._

import scala.math.Ordered._
import scalaz.NewType
import scalaz.Scalaz._

trait ArbitraryEvent extends ArbitraryTime {
  val Locations = for (i <- 0 to 10) yield "location" + i
  val Startups = for (i <- 0 to 50) yield "startup" + i
  val TwitterClients = for (i <- 0 to 10) yield "client" + i 
  val EventTypes = List("tweeted", "funded")

  val genGender = oneOf("male", "female")

  val genOtherStartups = for {
    i <- frequency((20, 0), (3, 1), (1, 2))
    startups <- containerOfN[List, String](i, oneOf(Startups))
  } yield startups

  case class EventData(value: JObject) extends NewType[JObject]

  case class Event(eventName: String, data: EventData, tags: List[Tag]) {
    def tagFields = tags.map {
      case Tag(name, TimeReference(_, time)) => 
        JField(Tag.tname(name), time.serialize)

      case Tag(name, Hierarchy(locations)) => 
        JField(Tag.tname(name), JObject(locations.collect { case Hierarchy.NamedLocation(name, path) => JField(name, path.path.serialize) }))

      case Tag(name, NameSet(values)) => sys.error("NameSet tags not yet tested.")
    }
    
    lazy val messageData = JObject(data.fields ::: tagFields)

    lazy val message = JObject(JField(eventName, messageData) :: Nil)

    def timestamp: Option[Instant] = tags collect { case Tag(name, TimeReference(_, time)) => time } headOption
  }

  val locations = List(
    "usa" -> List(
      "colorado" -> List(
        "boulder" -> List("80303", "80304", "80305"),
        "lakewood" -> List("80215")
      ),
      "arizona" -> List(
        "tucson" -> List("85716")
      )
    )
  )
  
  import Hierarchy._
  implicit val locGen: Gen[Hierarchy] = for {
    (country, states) <- oneOf(locations)
    val countryPath = Path(country) 

    (state, cities) <- oneOf(states)
    val statePath = countryPath / state

    (city, zips) <- oneOf(cities)
    val cityPath = statePath / city

    zip <- oneOf(zips)
    val zipPath = cityPath / zip
  } yield Hierarchy.of(
    List(NamedLocation("country", countryPath),
         NamedLocation("state", statePath),
         NamedLocation("city", cityPath),
         NamedLocation("zip", zipPath))
  ) | sys.error("can't happen")

  val fullEventGen = for {
    eventName      <- oneOf(EventTypes)
    time           <- genTime
    geoloc         <- locGen
    eventData      <- eventDataGen
  } yield Event(
    eventName, 
    eventData, 
    List(Tag("timestamp", TimeReference(AggregationEngine.timeSeriesEncoding, time)), Tag("location", geoloc))
  )
  
  implicit val eventDataGen = for {
    location       <- oneOf(Locations)
    retweet        <- oneOf(true, false)
    recipientCount <- choose(0, 3)
    startup        <- oneOf(Startups)
    gender         <- genGender
    otherStartups  <- genOtherStartups
    twitterClient  <- oneOf(TwitterClients)
    tweet          <- identifier
  } yield EventData(
    JObject(
      JField("location",       location.serialize) ::
      JField("retweet",        retweet.serialize) ::
      JField("gender",         gender) ::
      JField("recipientCount", recipientCount.serialize) ::
      JField("startup",        startup.serialize) ::
      JField("otherStartups",  otherStartups.serialize) ::
      JField("twitterClient",  twitterClient) ::
      JField("~tweet",         tweet) ::
      Nil
    )
  )

  def keysf(granularity: Periodicity): List[PartialFunction[Tag, String]] = List(
    { case Tag("timestamp", TimeReference(_, instant)) => granularity.floor(instant).toString },
    { case Tag("location", Hierarchy(locations)) => locations.sortBy(_.path.length).head.path.toString }
  )

  def timeSlice(l: List[Event], granularity: Periodicity) = {
    val timestamps = l.map(_.tags.collect{ case Tag("timestamp", TimeReference(_, time)) => time }.head)
    val sortedEvents = l.zip(timestamps).sortBy(_._2)
    val sliceLength = sortedEvents.size / 2
    val startTime = granularity.floor(sortedEvents.drop(sliceLength / 2).head._2)
    val endTime = granularity.ceil(sortedEvents.drop(sliceLength + (sliceLength / 2)).head._2)
    (sortedEvents collect { case (e, t) if t >= startTime && t < endTime => e }, startTime, endTime)
  }

  def valueCounts(l: List[Event]) = l.foldLeft(Map.empty[(JPath, JValue), Int]) {
    case (map, Event(eventName, obj, _)) =>
      obj.flattenWithPath.foldLeft(map) {
        case (map, (path, value)) =>
          val key = (JPath(eventName) \ path, value)
          map + (key -> (map.getOrElse(key, 0) + 1))
      }
  }

  def expectedCounts(events: List[Event], property: String, keys: List[Tag => String]) = {
    expectedSums(events, property, keys) mapValues {
      _.mapValues {
        case (k, v) => v 
      }
    }
  }

  def expectedMeans(events: List[Event], property: String, keys: List[Tag => String]): Map[String, Map[List[String], Double]] = {
    expectedSums(events, property, keys) mapValues {
      _.mapValues {
        case (k, v) => v / k
      }
    }
  }

  // returns a map from set of keys derived from the tags to count and sum
  def expectedSums(events: List[Event], property: String, keyf: List[Tag => String]): Map[String, Map[List[String], (Int, Double)]] = {
    events.foldLeft(Map.empty[String, Map[List[String], (Int, Double)]]) {
      case (map, Event(eventName, obj, tags)) =>
        val keys = tags zip keyf map { case (t, f) => f(t) }

        obj.flattenWithPath.foldLeft(map) {
          case (map, (path, value)) if path.nodes.last == JPathField(property) =>
            map + (
              eventName -> (
                map.get(eventName) match {
                  case None => Map(keys -> (1, value.deserialize[Int]))
                  case Some(totals) => 
                    totals.get(keys) match {
                      case None => totals + (keys -> (1, value.deserialize[Double]))
                      case Some((count, sum)) =>
                        totals + (keys -> (count + 1, value.deserialize[Double] + sum))
                    }
                }
              )
            )
            
          case (map, _) => map
        }
    }   
  }
}

object ArbitraryEvent extends ArbitraryEvent {
  val genTimeClock = Clock.System
}
