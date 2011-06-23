import sbt._
import Keys._
//import ProguardPlugin._
import OneJarPlugin._
import AltDependency._

object ServicesSettings {
  val buildOrganization = "com.reportgrid"
  val buildVersion = "0.2"
  val buildScalaVersion = "2.9.0-1"
  
  val serviceSettings = Defaults.defaultSettings ++ Seq (
    organization := buildOrganization,
    version      := buildVersion,
    scalaVersion := buildScalaVersion
  )
}

object ServicesBuild extends Build {
  import ServicesSettings._
  import AltDependency._

  val client   = GitAltDependency(_: java.io.File, file("../client-libraries/scala"), ProjectRef(uri("git://github.com/reportgrid/client-libraries"), "scala"))
  val blueeyes = GitAltDependency(_: java.io.File, file("../blueeyes"), RootProject(uri("git://github.com/jdegoes/blueeyes")))

  override def projectDefinitions(base: File) = {
    val common = Project("common", file("common"), 
      settings = serviceSettings ++ Seq(libraryDependencies += "joda-time" % "joda-time" % "1.6.2")
    )

    val instrumentation = Project("instrumentation", file("instrumentation"), settings = serviceSettings) dependsOnAlt(blueeyes(base)) dependsOnAlt(client(base))

    val analyticsSettings = serviceSettings ++ Seq( 
      libraryDependencies ++= Seq(
        "joda-time"               % "joda-time"    % "1.6.2",
        "org.scalaz"              %% "scalaz-core" % "6.0.1",
        "org.scala-tools.testing" %% "specs"       % "1.6.8"  % "test",
        "org.scala-tools.testing" %% "scalacheck"  % "1.9"    % "test"),
      mainClass := Some("com.reportgrid.analytics.AnalyticsServer")
    )

    val analytics = Project("analytics", file("analytics"), settings = analyticsSettings ++ oneJarSettings) dependsOn(common) dependsOnAlt (blueeyes(base))

    val benchmarkSettings = serviceSettings ++ Seq(
      libraryDependencies += "org.scala-tools.testing" %% "scalacheck"  % "1.9" % "compile",
      mainClass := Some("com.reportgrid.benchmark.AnalyticsBenchmark")
    )

    val benchmark = Project("benchmark", file("benchmark"), settings = benchmarkSettings ++ oneJarSettings) dependsOn(common) dependsOnAlt(blueeyes(base)) dependsOnAlt(client(base))

    val services = Project("services", file(".")) aggregate (common, analytics, benchmark) 
    common :: analytics :: benchmark :: services :: Nil
  }
}
