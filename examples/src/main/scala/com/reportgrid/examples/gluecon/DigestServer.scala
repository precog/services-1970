package com.reportgrid.examples.gluecon

import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JPath
import blueeyes.util.CommandLineArguments

import dispatch._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpGet
import org.codehaus.jackson._
import org.codehaus.jackson.map._
import net.lag.configgy.Configgy

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scalaz._
import Scalaz._

import com.reportgrid.api.blueeyes.ReportGrid
import com.reportgrid.api._

import java.net.URI
import java.net.URL
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants
import javax.xml.stream.XMLStreamReader
import org.joda.time.DateTime
import nlp.Stemmer

object DigestServer {
  def main(argv: Array[String]) {
    val args = CommandLineArguments(argv: _*)

    if (args.size == 0 || !args.parameters.get("help").isEmpty) {
      println("Usage: --configFile [config file]")
            
      System.exit(-1)
    } else {        
      Configgy.configure(args.parameters.get("configFile").getOrElse(error("Expected --configFile option")))
      
      val config = Configgy.config

      val gnipHost = for (host <- config.getString("gnipHost")) yield {
        new HttpHost(host, config.getInt("port", 80))      
      }

      val reportGridUrl = config.getString("reportGridUrl").map(new URL(_))
      val credentials = (config.getString("username") <**> config.getString("password"))(Credentials.apply)

      val http = new Http
    
      for {
        token <- config.getString("tokenId")
        host <- gnipHost
        path <- config.getString("gnipPath")
        creds <- credentials
      } {
        val gnipUrl = new URL("http://" + gnipHost + "/" + path)
        new GlueConGnipDigester(token).ingestGnipJsonStream(http, host, gnipUrl.toURI, creds)
      }
    }
  }
}

object GlueConGnipDigester {
  val podCompanies: Map[String, String] = Map(
    "report grid" -> "ReportGrid",
    "ReportGrid" -> "ReportGrid",
    "Sing.ly" -> "Sing.ly",
    "BigDoor" -> "BigDoor",
    "BigDoorMedia" -> "BigDoor",
    "StreamStep" -> "StreamStep",
    "WanderFly" -> "WanderFly",
    "Proxomo" -> "Proxomo",
    "LocVox" -> "LocVox",
    "Eclipse Foundation" -> "Eclipse Foundation",
    "Standing_Cloud" -> "Standing Cloud",
    "Standing Cloud" -> "Standing Cloud",
    "Flomio" -> "Flomio",
    "jexyco" -> "Jexy",
    "Axiomatics" -> "Axiomatics",
    "Get_Rainmaker" -> "Rainmaker",
    "rainmaker.cc" -> "Rainmaker",
    "StatsMix" -> "StatsMix",
    "Tendril" -> "Tendril",
    "Tendril Networks" -> "Tendril"
  ) map {
    case (k, v) => (Stemmer.stem(k.toLowerCase), v)
  }

  val allCompanies: Set[String] = Set(
  )

  def bucketCounts(i: Int) = {
    if (i / 25000 > 0) (i / 10000) * 10000
    else if (i / 2500 > 0) (i / 1000) * 1000
    else if (i / 250 > 0) (i / 100) * 100
    else if (i / 25 > 0) (i / 10) * 10
    else i
  }

  case class Tweet(
    startups: List[String],
    properties: JObject,
    time: Option[DateTime]
  )

  def parse(parser: JsonParser): Stream[Tweet] = {
    def cons: Stream[Tweet] = {
      parser.nextToken match {
        case JsonToken.START_OBJECT => 
          Stream.cons(extractTweet(parser), cons)

        case null => Stream.empty[Tweet]
        case tok => error("Got an unexpected token at the root; should only be objects here: " + tok + ": " + parser.getText)
      }
    }

    cons
  }

  def extractTweet(parser: JsonParser): Tweet = {
    val entry = parser.readValueAsTree
    val actor = Option(entry.get("actor")) 
    val obj   = Option(entry.get("object")) 

    val time = for (baseNode <- obj; postedTime <- Option(baseNode.get("postedTime")); value <- Option(postedTime.getTextValue)) yield new DateTime(value)
    val body = for (body <- Option(entry.get("body")); value <- Option(body.getTextValue)) yield value

    val client = for {
      genNode  <- Option(entry.get("generator"))
      nameNode <- Option(genNode.get("displayName"))
      value    <- Option(nameNode.getTextValue) 
    } yield value
    
    val words = body.toList.flatMap(_.toLowerCase.split("""\s+""").map(s => Stemmer.stem(s.replaceAll("@", ""))))
    val startups = words.flatMap(podCompanies.get) ++ words.filter(allCompanies)


    def locName(node: JsonNode) = for {
      loc <- Option(node.get("location"))
      name <- Option(loc.get("displayName"))
      value <- Option(loc.getTextValue)
    } yield value

    val location = locName(entry).orElse(actor.flatMap(locName))
    
    val followersCount = actor.flatMap(a => Option(a.get("followersCount")).flatMap(v => Option(v.getValueAsInt).map(bucketCounts)))
    val friendsCount = actor.flatMap(a => Option(a.get("friendsCount")).flatMap(v => Option(v.getValueAsInt).map(bucketCounts)))
    val clout = for (friends <- friendsCount; followers <- followersCount) yield bucketCounts(((followers.toDouble / friends.toDouble) * 10).toInt)
    val languages = actor.flatMap(a => Option(a.get("languages")).map(_.getElements.asScala.toSeq.flatMap(e => Option(e.getTextValue))))

    val Happy = """[:;B]-?[)D]""".r
    val Sad = """[:;]-?[(Pp]""".r
    val Surprised = """[:;]-?[oO]""".r
    val Excited = """\w+\s*!""".r

    val emotion = {
      val detected = words flatMap {
        case Happy => Some("happy")
        case Sad => Some("unhappy")
        case Surprised => Some("surprised")
        case Excited => Some("excited")
        case _ => None
      }

      if (detected.contains("happy")) Some("happy")
      else if (detected.contains("excited")) Some("excited")
      else if (detected.contains("surprised")) Some("surprised")
      else if (detected.contains("unhappy")) Some("unhappy")
      else None
    }

    def extract[T](path: String, opt: Option[T])(implicit f: T => JValue): JObject => JObject = (obj: JObject) => {
      opt.map(v => obj.set(JPath(path), f(v)).asInstanceOf[JObject]).getOrElse(obj)
    }
    
    val jstate =  init[JObject] <*
                  modify(extract("client", client.map(_.replaceAll("""\d\.""", "")))) <*
                  modify(extract("startups", startups.toNel.map(_.list))) <*
                  modify(extract("location", location)) <*
                  modify(extract("emotion", emotion)) <*
                  modify(extract("followersCount", followersCount)) <*
                  modify(extract("friendsCount", friendsCount)) <*
                  modify(extract("clout", clout)) <*
                  modify(extract("languages", languages))

    Tweet(startups, jstate(JObject(Nil))._1, time)
  }
}

class GlueConGnipDigester(tokenId: String) {
  import GlueConGnipDigester._
  val api = new ReportGrid(tokenId)


  def ingestGnipJsonStream(http: Http, host: HttpHost, uri: URI, credentials: Credentials): Unit = {
    val req = new HttpGet(uri)

    def handleStream(resp: HttpResponse): Unit = {
      for (entity <- Option(resp.getEntity)) {
        val jsonFactory = (new ObjectMapper).getJsonFactory()
        val parser = jsonFactory.createJsonParser(entity.getContent)
        for (Tweet(startups, properties, time) <- parse(parser)) {
          if (podCompanies.values.exists(startups.contains)) {
            sendToReportGrid("pods", properties, time.map(_.toDate))
          }

          sendToReportGrid("all", properties, time.map(_.toDate))
        }
      }
    }

    http.execute(host, Some(credentials), req, handleStream _, { case t: Throwable => ingestGnipJsonStream(http, host, uri, credentials) })
  }


//  def ingestGnipXmlStream(http: Http, host: HttpHost, uri: URI, credentials: Credentials): Unit = {
//    val req = new HttpGet(uri)
//
//    def handleStream(resp: HttpResponse): Unit = {
//      for (entity <- Option(resp.getEntity)) {
//        val factory = XMLInputFactory.newInstance();
//        val parser = factory.createXMLStreamReader(entity.getContent);
//        parse(parser)
//      }
//    }
//
//    @tailrec def parse(parser: XMLStreamReader): Unit = {
//      parser.next match {
//        case XMLStreamConstants.START_ELEMENT if parser.getLocalName == "entry" => processEntry(parser, JObject(Nil))
//        case XMLStreamConstants.END_ELEMENT if parser.getLocalName == "entry"   => parse(parser)
//      }
//    }
//
//    def processEntry(parser: XMLStreamReader, jobject: JObject): Unit = {
//      parser.next match {
//        case XMLStreamConstants.START_ELEMENT => 
//      }
//    }
//
//    http.execute(host, Some(credentials), req, handleStream _, { case t: Throwable => t.printStackTrace })
//  }

  def sendToReportGrid(path: String, jobject: JObject, time: Option[java.util.Date]) = api.track(
    path       = "/gluecon/" + path,
    name       = "tweet",
    properties = jobject,
    rollup     = true,
    timestamp  = time,
    count = Some(1)
  )
}


// vim: set ts=4 sw=4 et:
