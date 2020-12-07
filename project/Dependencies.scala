import sbt._

object Dependencies {

  val sparkVersion = "3.0.1"

  val testsDependencies = Seq(
    "org.scalatest" %% "scalatest" % "3.2.2",
    "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0",
    "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_1.0.0"
  )

  val organizationsToExclude: Seq[ExclusionRule] = testsDependencies.map(_.organization).filter(!_.contains("spark")).map(org => ExclusionRule(organization = org))

  val providedDependencies = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.spark" %% "spark-yarn" % sparkVersion
  )

  val embeddedDependencies: Seq[ModuleID] = Seq(
    //    "org.apache.spark" %% "spark-core" % sparkVersion,
    //    "org.apache.spark" %% "spark-sql" % sparkVersion,
    //    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    //    "org.apache.spark" %% "spark-yarn" % sparkVersion,
    "io.delta" %% "delta-core" % "0.7.0",
    "commons-cli" % "commons-cli" % "1.2",
    "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % "3.0.1",
    "mysql" % "mysql-connector-java" % "5.1.16"

  ).map(_.excludeAll(organizationsToExclude: _*))

  val rootDependencies: Seq[sbt.ModuleID] = embeddedDependencies ++ providedDependencies.map(_ % Provided) ++ testsDependencies.map(_ % Test)

  val mainRunnerDependencies: Seq[ModuleID] = (embeddedDependencies ++ providedDependencies ++ testsDependencies).map(_ % Compile)

}