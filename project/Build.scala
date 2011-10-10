import sbt._
import Keys._
import AltDependency._
import sbtassembly.Plugin.AssemblyKeys._
import sbt.NameFilter._

object ServicesSettings {
  val buildOrganization = "com.reportgrid"
  val buildVersion = "1.1.1-SNAPSHOT"
  val buildScalaVersion = "2.9.1"
  
  val serviceSettings = Defaults.defaultSettings ++ Seq (
    organization := buildOrganization,
    version      := buildVersion,
    scalaVersion := buildScalaVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked")
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


    val analyticsSettings = serviceSettings ++ Seq( 
      libraryDependencies ++= Seq(
        "joda-time"               % "joda-time"    % "1.6.2",
        "org.scalaz"              %% "scalaz-core" % "6.0.2",
        "org.scala-tools.testing" %% "specs"       % "1.6.9"  % "test",
        "org.scala-tools.testing" %% "scalacheck"  % "1.9"    % "test"
      ),
      mainClass := Some("com.reportgrid.analytics.AnalyticsServer"),
      jarName in assembly := "analytics-v1.jar"
    )

    val analytics = Project("analytics", file("analytics"), settings = sbtassembly.Plugin.assemblySettings ++ analyticsSettings) dependsOn(common) dependsOnAlt (blueeyes(base)) dependsOnAlt(client(base))


    val billingSettings = serviceSettings ++ sbtassembly.Plugin.assemblySettings ++ Seq(
      libraryDependencies ++= Seq(
        "org.scala-tools.testing" %% "specs"       % "1.6.9"  % "test",
        "org.scala-tools.testing" %% "scalacheck"  % "1.9"    % "test"
      ),
      mainClass := Some("com.reportgrid.billing.BillingServer"),
      jarName in assembly := "billing-v1.jar"
    )

    val billing = Project("billing", file("billing"), settings = billingSettings) dependsOnAlt blueeyes(base)


    val jessupSettings = serviceSettings ++ sbtassembly.Plugin.assemblySettings ++ Seq(
      libraryDependencies ++= Seq(
        "org.dspace.dependencies" % "dspace-geoip" % "1.2.3",
        "org.scala-tools.testing" %% "specs"       % "1.6.9"  % "test",
        "org.scala-tools.testing" %% "scalacheck"  % "1.9"    % "test"
      ),
      mainClass := Some("com.reportgrid.jessup.Server")
    )

    val jessup = Project("jessup", file("jessup"), settings = jessupSettings) dependsOnAlt blueeyes(base)


    val benchmarkSettings = serviceSettings ++ Seq(
      libraryDependencies += "org.scala-tools.testing" %% "scalacheck"  % "1.9" % "compile",
      mainClass := Some("com.reportgrid.benchmark.AnalyticsTool")
    )

    val benchmark = Project("benchmark", file("benchmark"), settings = benchmarkSettings ++ sbtassembly.Plugin.assemblySettings) dependsOn(common) dependsOnAlt(blueeyes(base)) dependsOnAlt(client(base))


    val services = Project("services", file(".")) aggregate (common, analytics, benchmark, billing, jessup) 

    common :: analytics :: benchmark :: billing :: jessup :: services :: Nil
  }
}
