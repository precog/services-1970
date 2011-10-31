package com.reportgrid.analytics

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._

class Path private (private val _path : String) {
  val path = cleanPath(_path)

  lazy val elements = path.split("/").toList

  def length = elements.length

  def + (that: Path) = this / that

  def - (that: Path): Path = if (this.path.startsWith(that.path)) {
    new Path(path.substring(that.path.length))
  } else {
    sys.error("This path is not a descendent of that path: this = " + this.toString + ", that = " + that.toString)
  }

  def / (that: Path) = new Path(this.path + "/" + that.path)

  def parent: Option[Path] = path.split("/").reverse.toList match {
    case Nil      => None
    case x :: Nil => Some(new Path("/"))
    case x :: xs  => Some(new Path(xs.reverse.mkString("/")))
  }

  def ancestors: List[Path] = {
    val parentList = parent.toList

    parentList ++ parentList.flatMap(_.ancestors)
  }

  def rollups(rollup: Boolean): List[Path] = if (rollup) this :: ancestors else List(this)

  def parentChildRelations: List[(Path, Path)] = {
    val a = ancestors

    a.zip(this :: a)
  }

  override def equals(that: Any) = that match {
    case that: Path => this.path == that.path

    case _ => false
  }

  override def hashCode = path.hashCode

  override def toString = path

  private def cleanPath(string: String): String = ("/" + string + "/").replaceAll("/+", "/")
}

trait PathSerialization {
    final implicit val PathDecomposer = new Decomposer[Path] {
    def decompose(v: Path): JValue = JString(v.toString)
  }

  final implicit val PathExtractor = new Extractor[Path] {
    def extract(v: JValue): Path = Path(v.deserialize[String])
  }
}

object Path extends PathSerialization {
  val Root = Path("/")

  implicit def stringToPath(string: String): Path = apply(string)

  def apply(path: String): Path = new Path(path)

  def unapply(path: Path): Option[String] = Some(path.path)
}
