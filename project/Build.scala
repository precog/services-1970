import sbt._
import Keys._
//import ProguardPlugin._
import OneJarPlugin._

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

  private def tryLocalGit(buildBase: java.io.File, p: Project, f: java.io.File, git: URI): Project = {
    val resolved = if (f.isAbsolute) f else new java.io.File(buildBase, f.getPath)
    val dep = if(resolved.isDirectory) RootProject(resolved) else RootProject(git)
    p dependsOn dep
  }

  private def tryLocalDep(buildBase: java.io.File, p: Project, f: java.io.File, dep: ModuleID): Project = {
   val resolved = if (f.isAbsolute) f else new java.io.File(buildBase, f.getPath)
   if(resolved.isDirectory) p dependsOn ( RootProject(resolved) )
   else                     p settings( libraryDependencies += dep )
  }

  override def projectDefinitions(base: File) = {
    val common = Project("common", file("common"), 
      settings = serviceSettings ++ Seq(
        libraryDependencies += "joda-time"               % "joda-time"    % "1.6.2"
      )
    )

    val analyticsSettings = serviceSettings ++ Seq( 
      libraryDependencies ++= Seq(
        "joda-time"               % "joda-time"    % "1.6.2",
        "org.scalaz"              %% "scalaz-core" % "6.0.1",
        "org.scala-tools.testing" %% "specs"       % "1.6.8"  % "test",
        "org.scala-tools.testing" %% "scalacheck"  % "1.9"    % "test"),
      mainClass := Some("com.reportgrid.analytics.AnalyticsServer")
    )

    val analytics = tryLocalGit(base,
      Project("analytics", file("analytics"), settings = analyticsSettings ++ oneJarSettings) dependsOn(common),
      file("../blueeyes"),
      uri("git://github.com/jdegoes/blueeyes")
    )

    val benchmarkSettings = serviceSettings ++ Seq(
      libraryDependencies ++= Seq(
        "com.reportgrid"          %% "blueeyes"    % "0.4.0" % "compile",
        "com.reportgrid"          %% "reportgrid-client" % "0.3.0" % "compile",
        "org.scala-tools.testing" %% "scalacheck"  % "1.9" % "compile"
      ),
      mainClass := Some("com.reportgrid.benchmark.AnalyticsBenchmark")
    )

    val benchmark = Project("benchmark", file("benchmark"), settings = benchmarkSettings ++ oneJarSettings) dependsOn(common)

    val services = Project("services", file(".")) aggregate (common, analytics, benchmark) 
    common :: analytics :: benchmark :: services :: Nil
  }
}
