package com.reportgrid.billing

import blueeyes.json.JsonAST._
import blueeyes.concurrent.Future
import blueeyes.core.data.ByteChunk
import blueeyes.core.data.BijectionsChunkString._
import blueeyes.core.service.HttpClient
import blueeyes.core.http.HttpResponse
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.core.http.MimeTypes._

trait Mailer {
  def sendEmail(from: String, to: Array[String], cc: Array[String], bcc: Array[String], subject: String, body: String): Future[Option[String]]
}

class OverrideMailTo(overrideTo: Array[String], delegate: Mailer) extends Mailer {
  def sendEmail(from: String, to: Array[String], cc: Array[String], bcc: Array[String], subject: String, body: String): Future[Option[String]] = {
    delegate.sendEmail(from, overrideTo, cc, bcc, subject, body)
  }
}

class SendGridMailer(client: HttpClient[ByteChunk], url: String, apiUser: String, apiKey: String) extends Mailer {

  def sendEmail(from: String, to: Array[String], cc: Array[String], bcc: Array[String], subject: String, body: String): Future[Option[String]] = {
    val params: List[(String, Option[String])] =
      ("api_user", Some(apiUser)) ::
        ("api_key", Some(apiKey)) ::
        ("from", Some(from)) ::
        ("subject", Some(subject)) ::
        ("text", Some(body)) ::
        Nil

    val pclient = params.foldLeft(client)(addQuery)

    val arrayParams: List[(String, Option[Array[String]])] =
      ("to", Some(to)) ::
        ("cc", Some(cc)) ::
        ("bcc", Some(bcc)) ::
        Nil

    val pclient2 = arrayParams.foldLeft(pclient)(addQueryList)

    val result: Future[HttpResponse[String]] =
      pclient2.
        contentType[ByteChunk](application / json).
        post[String](url)("")

    result.map(h => h.content)

  }

  def addQuery(client: HttpClient[ByteChunk], key: String, value: String): HttpClient[ByteChunk] = {
    client.query(key, value)
  }

  def addQuery(client: HttpClient[ByteChunk], kv: (String, Option[String])): HttpClient[ByteChunk] = {
    kv._2.map(client.query(kv._1, _)).getOrElse(client)
  }

  def addQueryList(client: HttpClient[ByteChunk], kv: (String, Option[Array[String]])): HttpClient[ByteChunk] = {
    val vals: List[(String, Option[String])] = kv._2.getOrElse(Array()).toList.map(v => (kv._1 + "[]", Some(v)))
    vals.foldLeft(client)(addQuery)
  }
}

class NullMailer extends Mailer {
  def sendEmail(from: String, to: Array[String], cc: Array[String], bcc: Array[String], title: String, content: String): Future[Option[String]] = Future.sync(None)

}
