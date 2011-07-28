package com.reportgrid.analytics

import org.joda.time.Instant
import scalaz.Scalaz._
import blueeyes.json.JsonAST._
import blueeyes.json.Printer._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._

sealed trait TimeSpan {
  def finite: Option[TimeSpan.Finite]
}
object TimeSpan {
  def apply(start: Instant, end: Instant) = Finite(start, end)

  case class Finite(start: Instant, end: Instant) extends TimeSpan {
    def finite = Some(this)
  }

  case object Eternity extends TimeSpan {
    def finite = None
  }
}



// vim: set ts=4 sw=4 et:
