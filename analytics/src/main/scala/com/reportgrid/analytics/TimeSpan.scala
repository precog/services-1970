package com.reportgrid.analytics

import org.joda.time.Instant
import scalaz.Scalaz._
import blueeyes.json.JsonAST._
import blueeyes.json.Printer._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._

case class TimeSpan(start: Instant, end: Instant)



// vim: set ts=4 sw=4 et:
