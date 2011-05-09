package com.reportgrid.analytics

import blueeyes.json.JPath

case class Variable(name: JPath) {
  def parent: Option[Variable] = name.parent.map { parent => copy(name = parent) }
}