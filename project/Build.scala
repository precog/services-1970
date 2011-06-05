import sbt._
import Keys._
import Defaults.defaultSettings
//import ProguardPlugin._
import OneJarPlugin._

object ServicesBuild extends Build {
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
      Project("common", file("common")),
      file("../blueeyes"),
      uri("https://github.com/jdegoes/blueeyes")
    )

    val analyticsSettings = Seq(
      mainClass := Some("com.reportgrid.analytics.AnalyticsServer")
    )

    val analytics = Project("analytics", file("analytics"), settings = defaultSettings ++ analyticsSettings ++ oneJarSettings) dependsOn(common)


    val benchmarkSettings = Seq(
      libraryDependencies += "org.scala-tools.testing" % "scalacheck_2.8.0"  % "1.7",
      mainClass := Some("com.reportgrid.benchmark.AnalyticsBenchmark")
    )

    val benchmark = tryLocalDep(base,
      Project("benchmark", file("benchmark"), settings = defaultSettings ++ benchmarkSettings ++ oneJarSettings) dependsOn(common),
      file("../client-libraries/scala"),
      "com.reportgrid" % "client-libraries" % "0.0.2"
    )

    val services = Project("services", file(".")) aggregate (common, analytics, benchmark)

    common :: analytics :: benchmark :: services :: Nil
  }
}
