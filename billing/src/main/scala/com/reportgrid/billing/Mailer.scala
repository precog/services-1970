package com.reportgrid.billing

import scalaz._
import scalaz.Scalaz._
import blueeyes.concurrent.Future

trait Mailer {
  def send(): Future[Validation[String, Unit]]
}

class SendMailer extends Mailer {
  def send(): Future[Validation[String, Unit]] = Future.sync(Failure("Not yet implemented"))
}

class NullMailer extends Mailer {
  def send(): Future[Validation[String, Unit]] = Future.sync(Success(()))
}