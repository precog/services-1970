package com.reportgrid.analytics

import blueeyes.util._
import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.DefaultSerialization._

import org.joda.time.Instant
import org.scalacheck.{Gen, Arbitrary}
import Gen._

import scala.math.Ordered._

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

  case class Event(data: JObject, timestamp: Instant) {
    def message = JObject(
      JField("events", data) :: JField("timestamp", timestamp.getMillis) :: Nil
    )
  }
  
  implicit val eventGen = for {
    eventName      <- oneOf(EventTypes)
    location       <- oneOf(Locations)
    time           <- genTime
    retweet        <- oneOf(true, false)
    recipientCount <- choose(0, 3)
    startup        <- oneOf(Startups)
    gender         <- genGender
    otherStartups  <- genOtherStartups
    twitterClient  <- oneOf(TwitterClients)
    tweet          <- identifier
  } yield Event(
    JObject(
      JField(eventName, JObject(
        JField("location",       location.serialize) ::
        JField("retweet",        retweet.serialize) ::
        JField("gender",         gender) ::
        JField("recipientCount", recipientCount.serialize) ::
        JField("startup",        startup.serialize) ::
        JField("otherStartups",  otherStartups.serialize) ::
        JField("twitterClient",  twitterClient) ::
        JField("~tweet",         tweet) ::
        Nil
      )) :: Nil
    ),
    time
  )

  def timeSlice(l: List[Event], granularity: Periodicity) = {
    val sortedEvents = l.sortBy(_.timestamp)
    val sliceLength = sortedEvents.size / 2
    val startTime = granularity.floor(sortedEvents.drop(sliceLength / 2).head.timestamp)
    val endTime = granularity.ceil(sortedEvents.drop(sliceLength + (sliceLength / 2)).head.timestamp)
    (sortedEvents.filter(t => t.timestamp >= startTime && t.timestamp < endTime), startTime, endTime)
  }

  def expectedCounts(events: List[Event], granularity: Periodicity, property: String) = {
    expectedSums(events, granularity, property) mapValues {
      _.mapValues {
        case (k, v) => v 
      }
    }
  }

  def expectedMeans(events: List[Event], granularity: Periodicity, property: String) = {
    expectedSums(events, granularity, property) mapValues {
      _.mapValues {
        case (k, v) => v / k
      }
    }
  }

  def expectedSums(events: List[Event], granularity: Periodicity, property: String) = {
    events.foldLeft(Map.empty[String, Map[Instant, (Int, Double)]]) {
      case (map, Event(JObject(JField(eventName, obj) :: Nil), time)) =>
        obj.flattenWithPath.foldLeft(map) {
          case (map, (path, value)) if path.nodes.last == JPathField(property) =>
            val instant = granularity.floor(time)
            map + (
              eventName -> (
                map.get(eventName) match {
                  case None => Map(instant -> (1, value.deserialize[Int]))
                  case Some(totals) => 
                    totals.get(instant) match {
                      case None => totals + (instant -> (1, value.deserialize[Double]))
                      case Some((count, sum)) =>
                        totals + (instant -> (count + 1, value.deserialize[Double] + sum))
                    }
                }
              )
            )
            
          case (map, _) => map
        }
    }   
  }
}

object ArbitraryEvent extends ArbitraryEvent
