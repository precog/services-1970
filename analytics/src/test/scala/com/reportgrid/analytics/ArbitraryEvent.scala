package com.reportgrid.analytics

import blueeyes.json.JsonAST._
import blueeyes.json.xschema.DefaultSerialization._

import org.joda.time.DateTime
import org.scalacheck.{Gen, Arbitrary}
import Gen._

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

  case class Event(data: JObject, timestamp: DateTime) {
    def message = JObject(
      JField("events", data) :: JField("timestamp", timestamp.getMillis) :: Nil
    )
  }
	
  implicit val eventGen = for {
    eventName      <- oneOf(EventTypes)
    location       <- oneOf(Locations)
    time 			     <- genTime
    retweet 		   <- oneOf(true, false)
    recipientCount <- choose(0, 3)
    startup 		   <- oneOf(Startups)
    otherStartups  <- genOtherStartups
    twitterClient  <- oneOf(TwitterClients)
  } yield Event(
    JObject(
      JField(eventName, JObject(
        JField("location", 			location.serialize) ::
        JField("retweet",			retweet.serialize) ::
        JField("recipientCount", 	recipientCount.serialize) ::
        JField("startup", 			startup.serialize) ::
        JField("otherStartups",     otherStartups.serialize) ::
        JField("twitterClient",     twitterClient) ::
        Nil
      )) :: Nil
    ),
    time
  )
}

object ArbitraryEvent extends ArbitraryEvent
