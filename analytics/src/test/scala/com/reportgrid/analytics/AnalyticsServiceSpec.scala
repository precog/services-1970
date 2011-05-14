package com.reportgrid.analytics

import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.persistence.mongo.{Mongo, MockMongo}
import blueeyes.core.http.{HttpStatus, HttpResponse, MimeTypes}
import blueeyes.json.JsonAST.{JValue, JObject, JField, JString, JNothing, JArray}
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.MimeTypes._
import blueeyes.persistence.mongo._
import blueeyes.core.http.MimeTypes._
import blueeyes.concurrent.Future
import blueeyes.core.data.{BijectionsChunkJson, BijectionsIdentity}
import blueeyes.core.service.HttpClient

import net.lag.configgy.{Configgy, ConfigMap}

import org.specs._
import org.scalacheck._
import Gen._

class AnalyticsServiceSpec extends BlueEyesServiceSpecification with ScalaCheck with AnalyticsService with ArbitraryEvent {

  def mongoFactory(config: ConfigMap): Mongo = new MockMongo()

//  override def configuration = """
//    services {
//      contactlist {
//        v1 {
//          mongo {
//            database{
//              contacts = "%s"
//            }
//            collection{
//              contacts = "%s"
//            }
//          }    
//        }
//      }
//    }
//    """.format(databaseName, collectionName)

  "Demo Service" should {
    "create events" in {
      val sampleEvents: List[Event] = containerOfN[List, Event](1000, eventGen).sample.get

      for (Event(name, jv) <- sampleEvents) {
        service.post[JValue](name)(jv)
      }

      val count = service.get[JValue]("/tweeted/count") 
      count.value must eventually {
        beLike {
          case Some(HttpResponse(_, _, Some(result), _)) =>
            println("Got :" + result)
            true
        }
      }
      

    }
  }
}
