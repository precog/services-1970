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

  val blueeyesDeps = com.samskivert.condep.Depends( 
    ("blueeyes",         null, "com.reportgrid"                  %% "blueeyes"         % "0.5.2-SNAPSHOT" changing())
  )

  val clientLibDeps = com.samskivert.condep.Depends(
    ("client-libraries", null, "com.reportgrid"                  %% "scala-client" % "0.3.1")
  )

  lazy val services = Project(id = "services", base = file(".")) aggregate(common, analytics, billing, jessup, vistrack)

  val commonSettings = nexusSettings ++ Seq(libraryDependencies ++= blueeyesDeps.libDeps)
  lazy val common = blueeyesDeps.addDeps(Project(id = "common", base = file("common")).settings(commonSettings: _*))

  val analyticsSettings = nexusSettings ++ Seq(libraryDependencies ++= clientLibDeps.libDeps)
  lazy val analytics = clientLibDeps.addDeps(Project(id = "analytics", base = file("analytics")).settings(analyticsSettings: _*).dependsOn(common))

  val billingSettings = nexusSettings ++ sbtassembly.Plugin.assemblySettings ++ Seq(libraryDependencies ++= clientLibDeps.libDeps)
  lazy val billing = clientLibDeps.addDeps(Project(id = "billing", base = file("billing")).settings(billingSettings: _*).dependsOn(common))

  val jessupSettings = nexusSettings ++ Seq(libraryDependencies ++= blueeyesDeps.libDeps)
  lazy val jessup = blueeyesDeps.addDeps(Project(id = "jessup", base = file("jessup")).settings(jessupSettings: _*))

  lazy val vistrack = Project(id = "vistrack", base = file("vistrack")).settings(nexusSettings: _*).dependsOn(common)

  val benchmarkSettings = nexusSettings ++ Seq(libraryDependencies ++= (clientLibDeps.libDeps ++ blueeyesDeps.libDeps))
  lazy val benchmark = blueeyesDeps.addDeps(clientLibDeps.addDeps(Project(id = "benchmark", base = file("benchmark")).settings(benchmarkSettings: _*).dependsOn(analytics)))
}
