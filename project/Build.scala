import sbt._
import Keys._
import AltDependency._
import sbtassembly.Plugin.AssemblyKeys._
import sbt.NameFilter._

object ServicesSettings {
  val buildOrganization = "com.reportgrid"
  val buildVersion = "1.2.1-SNAPSHOT"
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
    val commonSettings = serviceSettings ++ Seq(
        libraryDependencies ++= Seq(
          "joda-time" % "joda-time" % "1.6.2",
          "org.scalaz" %% "scalaz-core" % "6.0.2"
        )
      )

    val common = Project("common", file("common"), settings = commonSettings) dependsOnAlt(blueeyes(base)) 

    val analyticsSettings = serviceSettings ++ Seq( 
      libraryDependencies ++= Seq(
        "joda-time"               % "joda-time"           % "1.6.2",
        "org.scalaz"              %% "scalaz-core"        % "6.0.2",
        "org.scala-tools.testing" %% "specs"              % "1.6.9"  % "test",
        "org.scala-tools.testing" %% "scalacheck"         % "1.9"    % "test"
      ),
      mainClass := Some("com.reportgrid.analytics.AnalyticsServer")
      //, test in assembly := {}
    )

    val analytics = Project("analytics", file("analytics"), settings = sbtassembly.Plugin.assemblySettings ++ analyticsSettings) dependsOn(common) dependsOnAlt (blueeyes(base)) dependsOnAlt(client(base))

    val billingSettings = serviceSettings ++ sbtassembly.Plugin.assemblySettings ++ Seq(
      libraryDependencies ++= Seq(
        "commons-codec"           % "commons-codec"       % "1.5",
        "commons-httpclient"      % "commons-httpclient"  % "3.1",
        "joda-time"               % "joda-time"           % "1.6.2",
        "net.liftweb"             %% "lift-mapper"        % "2.4-M4",
        "org.mockito"             % "mockito-all"         % "1.9.0-rc1" % "test", 
        "org.scala-tools.testing" %% "specs"              % "1.6.9"     % "test",
        "org.scala-tools.testing" %% "scalacheck"         % "1.9"       % "test"
      ),
      mainClass := Some("com.reportgrid.billing.BillingServer")
    )

    val billing = Project("billing", file("billing"), settings = billingSettings) dependsOn(common) dependsOnAlt blueeyes(base)


    val jessupSettings = serviceSettings ++ sbtassembly.Plugin.assemblySettings ++ Seq(
      libraryDependencies ++= Seq(
        "org.dspace.dependencies" % "dspace-geoip" % "1.2.3",
        "org.scala-tools.testing" %% "specs"       % "1.6.9"  % "test",
        "org.scala-tools.testing" %% "scalacheck"  % "1.9"    % "test"
      ),
      mainClass := Some("com.reportgrid.jessup.Server")
    )

    val jessup = Project("jessup", file("jessup"), settings = jessupSettings) dependsOnAlt blueeyes(base)

    val services = Project("services", file(".")) aggregate (common, analytics, billing, jessup) 

    common :: analytics :: billing :: jessup :: services :: Nil
  }
}
