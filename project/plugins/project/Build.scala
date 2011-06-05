import sbt._
object PluginDef extends Build {
  override def projects = Seq(root)
  lazy val root = Project("plugins", file(".")) dependsOn(oneJar)
  lazy val oneJar = uri("git://github.com/reportgrid/xsbt-one-jar")
}

// vim: set ts=4 sw=4 et:
