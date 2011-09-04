package com.reportgrid.analytics

import blueeyes._
import blueeyes.json.JsonAST._
import blueeyes.json.{JPath, JPathIndex, JPathField}

import com.reportgrid.analytics._
import com.reportgrid.util.MapUtil._

import scala.collection.immutable.IndexedSeq
import scala.collection.immutable.Set
import scalaz.{Ordering => _, _}
import Scalaz._

case class Report[A <: Observation](tags: Seq[Tag], observations: Set[JointObservation[A]]) {
  def storageKeysets: Set[(Seq[StorageKeys], JointObservation[A])] = {
    for (joint <- observations; keys <- tags.map(_.value.storageKeys).sequence) 
    yield (keys -> joint)
  }
}
