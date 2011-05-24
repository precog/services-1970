package com.reportgrid.analytics

import blueeyes.json.JPath

case class Variable(name: JPath) {
  def parent: Option[Variable] = name.parent.map { parent => copy(name = parent) }
}

object Variable {
  implicit val orderingVariable: Ordering[Variable] = new Ordering[Variable] {
    override def compare(v1: Variable, v2: Variable) = {
      v1.name.toString.compare(v2.name.toString)
    }
  }
}
