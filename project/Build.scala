import sbt._
import Keys._
//import ProguardPlugin._
import OneJarPlugin._
import AltDependency._
import sbt.NameFilter._

object ServicesSettings {
  val buildOrganization = "com.reportgrid"
  val buildVersion = "0.3.2"
  val buildScalaVersion = "2.9.0-1"
  
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
        "org.scalaz"              %% "scalaz-core" % "6.0.1",
        "org.scala-tools.testing" %% "specs"       % "1.6.8"  % "test",
        "org.scala-tools.testing" %% "scalacheck"  % "1.9"    % "test"),
      mainClass := Some("com.reportgrid.analytics.AnalyticsServer")
    )

    val analytics = Project("analytics", file("analytics"), settings = analyticsSettings ++ oneJarSettings) dependsOn(common) dependsOnAlt (blueeyes(base)) dependsOnAlt(client(base))


    val yggdrasilSettings = serviceSettings ++ oneJarSettings ++ Seq(
      resolvers ++= Seq(
        //"riptano" at "http://mvn.riptano.com/content/repositories/public",
        //"Scale7 Maven Repo" at "https://github.com/s7/mvnrepo/raw/master"
        "Cloudera"       at "https://repository.cloudera.com/content/groups/cloudera-repos",
        "Java.net"       at "http://download.java.net/maven/2/",
        "HBase Thrift"   at "http://people.apache.org/~rawson/repo"
      ),
      libraryDependencies ++= Seq(
        //"org.scale7" % "scale7-pelops" % "1.2-0.8.x-SNAPSHOT",
        "org.apache.hbase"        % "hbase"        % "0.90.1-cdh3u0",
        "org.slf4j"               % "slf4j-api"    % "1.6.1", 
        "joda-time"               % "joda-time"    % "1.6.2",
        "org.scalaz"              %% "scalaz-core" % "6.0.1"
      ),
      mainClass := Some("com.reportgrid.yggdrasil.Yggdrasil"),
      oneJarExcludeJars := ((_: String).contains("slf4j-api-1.5.11.jar"))
    )

    val yggdrasil = Project("yggdrasil", file("yggdrasil"), settings = yggdrasilSettings ) dependsOn(common) dependsOnAlt (blueeyes(base))


    val benchmarkSettings = serviceSettings ++ Seq(
      libraryDependencies += "org.scala-tools.testing" %% "scalacheck"  % "1.9" % "compile",
      mainClass := Some("com.reportgrid.benchmark.AnalyticsTool")
    )

    val benchmark = Project("benchmark", file("benchmark"), settings = benchmarkSettings ++ oneJarSettings) dependsOn(common) dependsOnAlt(blueeyes(base)) dependsOnAlt(client(base))

    val services = Project("services", file(".")) aggregate (common, analytics, benchmark, yggdrasil) 
    common :: analytics :: benchmark :: yggdrasil :: services :: Nil
  }
}
