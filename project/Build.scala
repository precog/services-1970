import sbt._
import Keys._
import AltDependency._
import sbtassembly.Plugin.AssemblyKeys._
import sbt.NameFilter._

object ServicesSettings {
  val buildOrganization = "com.reportgrid"
  val buildScalaVersion = "2.9.1"
  
  val serviceSettings = Defaults.defaultSettings ++ Seq (
    organization := buildOrganization,
    scalaVersion := buildScalaVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    resolvers += "Scala-Tools Snapshots" at "http://scala-tools.org/repo-snapshots/"
  )
}

object ServicesBuild extends Build {
  import ServicesSettings._
  import AltDependency._

  val client   = GitAltDependency(_: java.io.File, file("../client-libraries/scala"), ProjectRef(uri("git://github.com/reportgrid/client-libraries"), "scala"))
  val blueeyes = GitAltDependency(_: java.io.File, file("../blueeyes"), RootProject(uri("git://github.com/jdegoes/blueeyes")))

  override def projectDefinitions(base: File) = {
    val commonSettings = serviceSettings ++ Seq(
        version      := "1.2.1-SNAPSHOT",
        libraryDependencies ++= Seq(
          "joda-time" % "joda-time" % "1.6.2",
          "org.scalaz" %% "scalaz-core" % "6.0.2",
          "org.specs2" %% "specs2"      % "1.7-SNAPSHOT"  % "test"
        )
      )

    val common = Project("common", file("common"), settings = commonSettings) dependsOnAlt(blueeyes(base)) 

    val analyticsSettings = serviceSettings ++ Seq( 
      version      := "1.3.2-SNAPSHOT",
      libraryDependencies ++= Seq(
        "joda-time"               % "joda-time"           % "1.6.2",
        "org.scalaz"              %% "scalaz-core"        % "6.0.2",
        "org.specs2"              %% "specs2"             % "1.7-SNAPSHOT"  % "test",
        "org.scala-tools.testing" %% "scalacheck"         % "1.9"    % "test"
      ),
      mainClass := Some("com.reportgrid.analytics.AnalyticsServer"), 
      parallelExecution in Test := false,
      test in assembly := {}
    )

    val analytics = Project("analytics", file("analytics"), settings = sbtassembly.Plugin.assemblySettings ++ analyticsSettings) dependsOn(common) dependsOnAlt(client(base))

    val billingSettings = serviceSettings ++ sbtassembly.Plugin.assemblySettings ++ Seq(
      version      := "1.1.2",
      libraryDependencies ++= Seq(
        "commons-codec"           % "commons-codec"       % "1.5",
        "commons-httpclient"      % "commons-httpclient"  % "3.1",
        "joda-time"               % "joda-time"           % "1.6.2",
        "org.scalaz"              %% "scalaz-core"        % "6.0.2",
        "org.specs2"              %% "specs2"             % "1.7-SNAPSHOT"  % "test",
        "org.scala-tools.testing" %% "scalacheck"         % "1.9"           % "test"
      ),
      mainClass := Some("com.reportgrid.billing.BillingServer")
    )

    val billing = Project("billing", file("billing"), settings = billingSettings) dependsOn(common) 

    val jessupSettings = serviceSettings ++ sbtassembly.Plugin.assemblySettings ++ Seq(
      version      := "1.0.1-SNAPSHOT",
      libraryDependencies ++= Seq(
        "org.dspace.dependencies" % "dspace-geoip" % "1.2.3",
        "org.specs2"              %% "specs2"      % "1.7-SNAPSHOT"  % "test",
        "org.scala-tools.testing" %% "scalacheck"  % "1.9"    % "test"
      ),
      mainClass := Some("com.reportgrid.jessup.Server")
    )

    val jessup = Project("jessup", file("jessup"), settings = jessupSettings) dependsOnAlt blueeyes(base)

    val vistrackSettings = serviceSettings ++ sbtassembly.Plugin.assemblySettings ++ Seq(
      version      := "1.0.0-SNAPSHOT",
      libraryDependencies ++= Seq(
        "commons-codec" % "commons-codec"         % "1.5",
        "org.specs2"              %% "specs2"     % "1.7-SNAPSHOT"  % "test",
        "org.scala-tools.testing" %% "scalacheck" % "1.9"    % "test"
      ),
      mainClass := Some("com.reportgrid.vistrack.VistrackServer")
    )

    val vistrack = Project("vistrack", file("vistrack"), settings = vistrackSettings) dependsOn(common) 

    val services = Project("services", file(".")) aggregate (analytics, billing, vistrack, jessup) 
    common :: analytics :: billing :: jessup :: vistrack :: services :: Nil
  }
}
