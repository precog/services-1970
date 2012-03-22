import sbt._
import Keys._
import sbtassembly.Plugin.AssemblyKeys._
import sbt.NameFilter._

object ServicesBuild extends Build {
  val nexusSettings = Defaults.defaultSettings ++ Seq (
    resolvers ++= Seq("Typesafe repo"            at   "http://repo.typesafe.com/typesafe/releases/",
                      "ReportGrid repo"          at   "http://nexus.reportgrid.com/content/repositories/releases",
                      "ReportGrid repo (public)" at   "http://nexus.reportgrid.com/content/repositories/public-releases",
                      "ReportGrid snapshot repo"          at   "http://nexus.reportgrid.com/content/repositories/snapshots",
                      "ReportGrid snapshot repo (public)" at   "http://nexus.reportgrid.com/content/repositories/public-snapshots"),

    publishTo <<= (version) { version: String =>
      val nexus = "http://nexus.reportgrid.com/content/repositories/"
      if (version.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus+"snapshots/") 
      else                                   Some("releases"  at nexus+"releases/")
    },

    credentials += Credentials(Path.userHome / ".ivy2" / ".rgcredentials")
  )

  test in assembly := {}

  val blueeyes = Seq("com.reportgrid" %% "blueeyes" % "0.5.3-SNAPSHOT" changing())

  val clientLib = Seq("com.reportgrid" %% "scala-client" % "0.3.3-SNAPSHOT")

  lazy val services = Project(id = "services", base = file(".")) aggregate(common, analytics, jessup, vistrack ,billing)

  val commonSettings = nexusSettings ++ Seq(libraryDependencies ++= blueeyes)
  lazy val common = Project(id = "common", base = file("common")).settings(commonSettings: _*)

  val analyticsSettings = nexusSettings ++ Seq(libraryDependencies ++= clientLib)
  lazy val analytics = Project(id = "analytics", base = file("analytics")).settings(analyticsSettings: _*).dependsOn(common)

  val billingSettings = nexusSettings ++ sbtassembly.Plugin.assemblySettings ++ Seq(libraryDependencies ++= clientLib)
  lazy val billing = Project(id = "billing", base = file("billing")).settings(billingSettings: _*).dependsOn(common)

  val jessupSettings = nexusSettings ++ Seq(libraryDependencies ++= blueeyes)
  lazy val jessup = Project(id = "jessup", base = file("jessup")).settings(jessupSettings: _*)

  lazy val vistrack = Project(id = "vistrack", base = file("vistrack")).settings(nexusSettings: _*).dependsOn(common)

  val benchmarkSettings = nexusSettings ++ Seq(libraryDependencies ++= (clientLib ++ blueeyes))
  lazy val benchmark = Project(id = "benchmark", base = file("benchmark")).settings(benchmarkSettings: _*).dependsOn(analytics)
}
