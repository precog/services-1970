name := "common"

version      := "1.2.1-SNAPSHOT"

organization := "com.reportgrid"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked")

libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "1.6.2",
  "org.scalaz" %% "scalaz-core" % "6.0.2",
  "org.specs2" %% "specs2"      % "1.7"  % "test"
)
