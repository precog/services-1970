import sbt._

class Plugins(info: ProjectInfo) extends PluginDefinition(info) {
  val commonsIo = "commons-io" % "commons-io" % "2.0"  
}