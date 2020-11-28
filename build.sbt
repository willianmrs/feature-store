import ReleaseTransformations._
val compilerVersion = "2.12.12"

val buildSettings = Seq(
  name := "Ifood Data Ingestion",
  scalaVersion := compilerVersion,
  description := "Ifood Data Ingestion",
  organization := "br.com.ifood.data.featurestore.data_ingestion",
  organizationName := "ifood.com.br",
  homepage := Some(url("http://some-doc.com.br/")),
  resolvers := Resolvers.defaultResoulvers,
  libraryDependencies ++= Dependencies.rootDependencies,
  javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
  run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run)).evaluated,
  fork := true,
  parallelExecution in Test := false,
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    setNextVersion,
    commitNextVersion,
    pushChanges
  )
)

lazy val root = (project in file(".")).settings(buildSettings: _*).settings(baseAssemblySettings: _*)
