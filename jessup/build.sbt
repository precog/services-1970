import AssemblyKeys._

name := "jessup"

version      := "1.0.1-SNAPSHOT"

organization := "com.reportgrid"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked")

libraryDependencies ++= Seq(
  "org.dspace.dependencies" % "dspace-geoip" % "1.2.3",
  "org.specs2"              %% "specs2"      % "1.7"  % "test",
  "org.scala-tools.testing" %% "scalacheck"  % "1.9"  % "test"
)

mainClass := Some("com.reportgrid.jessup.Server")

seq(assemblySettings: _*)
