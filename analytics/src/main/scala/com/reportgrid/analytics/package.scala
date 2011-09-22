package com.reportgrid

import _root_.blueeyes.json.JsonAST._
import _root_.blueeyes.json.JPath
import _root_.blueeyes.json.JPathField
import _root_.blueeyes.json.Printer._

package object analytics {
  def cleanPath(string: String): String = "/" + string.split("/").map(_.trim).filter(_.length > 0).mkString("/")

  implicit def jpath2rich(jpath: JPath): RichJPath = new RichJPath(jpath)
  class RichJPath(jpath: JPath) {
    def endsInInfiniteValueSpace = jpath.nodes.exists {
      case JPathField(name) => name startsWith "~"
      case _ => false
    }

    def endsInTagSpace = jpath.nodes.exists {
      case JPathField(name) => name startsWith Tag.Prefix
      case _ => false
    }
  }
}
