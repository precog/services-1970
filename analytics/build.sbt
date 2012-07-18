import AssemblyKeys._

name := "analytics"

version      := "1.3.17-SNAPSHOT"

organization := "com.reportgrid"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked")

compileOrder := CompileOrder.JavaThenScala

libraryDependencies ++= Seq(
  "com.reportgrid"          %% "blueeyes"           % "0.5.3-SNAPSHOT" changing(),
  "joda-time"               % "joda-time"           % "1.6.2",
  "ch.qos.logback"          % "logback-classic"     % "1.0.0",
  "org.scalaz"              %% "scalaz-core"        % "6.0.2",
  "org.specs2"              %% "specs2"             % "1.8"    % "test",
  "org.scala-tools.testing" %% "scalacheck"         % "1.9"    % "test"
)

mainClass := Some("com.reportgrid.analytics.AnalyticsServer")

parallelExecution in Test := false

seq(assemblySettings: _*)

test in assembly := {}

