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
    val common = tryLocalGit(base, 
      Project("common", file("common"), settings = serviceSettings),
      file("../blueeyes"),
      uri("git://github.com/jdegoes/blueeyes")
    )


    val analyticsSettings = serviceSettings ++ Seq( 
      libraryDependencies ++= Seq(
        "joda-time"               % "joda-time"    % "1.6.2",
        "org.scalaz"              %% "scalaz-core" % "6.0.1",
        "org.scala-tools.testing" %% "specs"       % "1.6.8"  % "test",
        "org.scala-tools.testing" %% "scalacheck"  % "1.9"    % "test"),
      mainClass := Some("com.reportgrid.analytics.AnalyticsServer")
    )

    val analytics = Project("analytics", file("analytics"), settings = analyticsSettings ++ oneJarSettings) dependsOn(common)


    val benchmarkSettings = serviceSettings ++ Seq(
      libraryDependencies += "org.scala-tools.testing" %% "scalacheck"  % "1.9" % "compile",
      mainClass := Some("com.reportgrid.benchmark.AnalyticsBenchmark")
    )

    val benchmark = tryLocalDep(base,
      Project("benchmark", file("benchmark"), settings = benchmarkSettings ++ oneJarSettings) dependsOn(common),
      file("../client-libraries/scala"),
      "com.reportgrid" % "client-libraries" % "0.0.2"
    )


    val services = Project("services", file(".")) aggregate (common, analytics, benchmark) 
    common :: analytics :: benchmark :: services :: Nil
  }
}
