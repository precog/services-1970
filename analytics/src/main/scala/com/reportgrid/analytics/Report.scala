package com.reportgrid.analytics

import scala.collection.immutable.Set
import scalaz.Scalaz._

case class Report[A <: Observation](tags: Seq[Tag], observations: Set[JointObservation[A]]) {
  def storageKeysets: Set[(Seq[StorageKeys], JointObservation[A])] = {
    for (joint <- observations; keys <- tags.toList.powerset.flatMap(_.map(_.value.storageKeys).sequence))
    yield (keys -> joint)
  }
}
