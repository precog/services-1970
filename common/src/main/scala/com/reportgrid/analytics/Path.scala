package com.reportgrid.analytics

class Path private (private val path_ : String) {
  val path = cleanPath(path_)

  lazy val elements = path.split("/")

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

object Path {
  val Root = Path("/")

  implicit def stringToPath(string: String): Path = apply(string)

  def apply(path: String): Path = new Path(path)

  def unapply(path: Path): Option[String] = Some(path.path)
}
