
import AssemblyKeys._

name := "benchmark"

version      := "1.1.4"

organization := "com.reportgrid"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked")

libraryDependencies ++= Seq(
  "com.reportgrid" %% "rosetta-json"           % "0.3.5",
  "org.scala-tools.testing" %% "scalacheck"    % "1.9"
)

mainClass := Some("com.reportgrid.benchmark.AnalyticsBenchmark")

seq(assemblySettings: _*)
