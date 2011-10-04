import sbt._
object PluginDef extends Build {
  lazy val root = Project("plugins", file(".")) dependsOn (assembly, altDep)
  lazy val assembly = RootProject(uri("git://github.com/eed3si9n/sbt-assembly.git#070"))
  lazy val altDep = RootProject(uri("git://github.com/reportgrid/xsbt-alt-deps")) 
}

// vim: set ts=4 sw=4 et:
