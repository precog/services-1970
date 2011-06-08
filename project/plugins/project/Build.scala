import sbt._
object PluginDef extends Build {
  private def tryLocalGit(buildBase: java.io.File, p: Project, f: java.io.File, git: URI): Project = {
    val resolved = if (f.isAbsolute) f else new java.io.File(buildBase, f.getPath)
    val dep = if(resolved.isDirectory) RootProject(resolved) else RootProject(git)
    p dependsOn dep
  }

  override def projectDefinitions(base: java.io.File) = {
    val root = Project("plugins", file(".")) 

    tryLocalGit(base, root, file("../../../xsbt-one-jar"), uri("git://github.com/reportgrid/xsbt-one-jar")) :: Nil
  }
}

// vim: set ts=4 sw=4 et:
