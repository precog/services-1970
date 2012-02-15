import AssemblyKeys._

name := "vistrack"

version      := "1.0.0-SNAPSHOT"

organization := "com.reportgrid"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked")

libraryDependencies ++= Seq(
  "commons-codec" % "commons-codec"         % "1.5",
  "org.specs2"              %% "specs2"     % "1.8"  % "test",
  "org.scala-tools.testing" %% "scalacheck" % "1.9"  % "test"
)

mainClass := Some("com.reportgrid.vistrack.VistrackServer")

seq(assemblySettings: _*)
