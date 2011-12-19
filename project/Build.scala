import sbt._
import Keys._
import sbtassembly.Plugin.AssemblyKeys._
import sbt.NameFilter._

object ServicesSettings {
  val buildOrganization = "com.reportgrid"
  val buildScalaVersion = "2.9.1"
  
  val serviceSettings = Defaults.defaultSettings ++ Seq (
    organization := buildOrganization,
    scalaVersion := buildScalaVersion,
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    resolvers ++= Seq("ReportGrid repo" at                   "http://nexus.reportgrid.com/content/repositories/releases",
			"ReportGrid snapshot repo" at          "http://nexus.reportgrid.com/content/repositories/snapshots",
			"ReportGrid public repo" at            "http://nexus.reportgrid.com/content/repositories/public-releases",
			"ReportGrid public snapshot repo" at   "http://nexus.reportgrid.com/content/repositories/public-snapshots",
		        "Typesafe repo" at                     "http://repo.typesafe.com/typesafe/releases/"),
    publishTo <<= (version) { version: String =>
      val nexus = "http://nexus.reportgrid.com/content/repositories/"
      if (version.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus+"snapshots/") 
      else                                   Some("releases"  at nexus+"releases/")
    },
    credentials += Credentials(Path.userHome / ".ivy2" / ".rgcredentials")
  )
}

object ServicesBuild extends Build {
  import ServicesSettings._

  val blueeyesDeps = com.samskivert.condep.Depends( 
    ("blueeyes",            null, "com.reportgrid" %% "blueeyes"     % "0.5.0-SNAPSHOT")
  )

  val clientDeps = com.samskivert.condep.Depends(
    ("client-libraries", "scala", "com.reportgrid" %% "scala-client" % "0.3.1")
  )

  override def projectDefinitions(base: File) = {
    val commonSettings = serviceSettings ++ Seq(
        version      := "1.2.1-SNAPSHOT",
        libraryDependencies ++= Seq(
          "joda-time" % "joda-time" % "1.6.2",
          "org.scalaz" %% "scalaz-core" % "6.0.2",
          "org.specs2" %% "specs2"      % "1.7-SNAPSHOT"  % "test"
        )
      )

    val common = blueeyesDeps.addDeps(Project("common", file("common"), settings = commonSettings).settings(libraryDependencies ++= blueeyesDeps.libDeps))

    val analyticsSettings = serviceSettings ++ Seq( 
      version      := "1.3.5-SNAPSHOT",
      libraryDependencies ++= (Seq(
        "joda-time"               % "joda-time"           % "1.6.2",
        "ch.qos.logback"          % "logback-classic"     % "1.0.0",
        "org.scalaz"              %% "scalaz-core"        % "6.0.2",
        "org.specs2"              %% "specs2"             % "1.7-SNAPSHOT"  % "test",
        "org.scala-tools.testing" %% "scalacheck"         % "1.9"    % "test"
      ) ++ blueeyesDeps.libDeps ++ clientDeps.libDeps),
      mainClass := Some("com.reportgrid.analytics.AnalyticsServer"), 
      parallelExecution in Test := false,
      test in assembly := {}
    )

    val analytics = ((blueeyesDeps.addDeps _) andThen (clientDeps.addDeps _))(Project("analytics", file("analytics"), settings = sbtassembly.Plugin.assemblySettings ++ analyticsSettings) dependsOn(common))

    val billingSettings = serviceSettings ++ sbtassembly.Plugin.assemblySettings ++ Seq(
      version      := "1.1.4",
      libraryDependencies ++= (Seq(
        "commons-codec"           % "commons-codec"       % "1.5",
        "commons-httpclient"      % "commons-httpclient"  % "3.1",
        "joda-time"               % "joda-time"           % "1.6.2",
        "org.scalaz"              %% "scalaz-core"        % "6.0.2",
        "ch.qos.logback"          % "logback-classic"     % "1.0.0",
        "org.specs2"              %% "specs2"             % "1.7-SNAPSHOT"  % "test",
        "org.scala-tools.testing" %% "scalacheck"         % "1.9"           % "test"
      ) ++ blueeyesDeps.libDeps),
      mainClass := Some("com.reportgrid.billing.BillingServer")
    )

    val billing = blueeyesDeps.addDeps(Project("billing", file("billing"), settings = billingSettings) dependsOn(common))

    val jessupSettings = serviceSettings ++ sbtassembly.Plugin.assemblySettings ++ Seq(
      version      := "1.0.1-SNAPSHOT",
      libraryDependencies ++= (Seq(
        "org.dspace.dependencies" % "dspace-geoip" % "1.2.3",
        "org.specs2"              %% "specs2"      % "1.7-SNAPSHOT"  % "test",
        "org.scala-tools.testing" %% "scalacheck"  % "1.9"    % "test"
      ) ++ blueeyesDeps.libDeps),
      mainClass := Some("com.reportgrid.jessup.Server")
    )

    val jessup = blueeyesDeps.addDeps(Project("jessup", file("jessup"), settings = jessupSettings))

    val vistrackSettings = serviceSettings ++ sbtassembly.Plugin.assemblySettings ++ Seq(
      version      := "1.0.0-SNAPSHOT",
      libraryDependencies ++= (Seq(
        "commons-codec" % "commons-codec"         % "1.5",
        "org.specs2"              %% "specs2"     % "1.7-SNAPSHOT"  % "test",
        "org.scala-tools.testing" %% "scalacheck" % "1.9"    % "test"
      ) ++ blueeyesDeps.libDeps),
      mainClass := Some("com.reportgrid.vistrack.VistrackServer")
    )

    val vistrack = blueeyesDeps.addDeps(Project("vistrack", file("vistrack"), settings = vistrackSettings) dependsOn(common))

    val services = Project("services", file(".")).aggregate(analytics, billing, vistrack, jessup).settings(serviceSettings :_*)
    common :: analytics :: billing :: jessup :: vistrack :: services :: Nil
  }
}
