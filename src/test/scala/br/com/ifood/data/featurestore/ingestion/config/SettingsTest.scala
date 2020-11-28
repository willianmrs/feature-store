//package br.com.ifood.data.featurestore.ingestion.config
//
//import org.scalatest.{FlatSpec, Matchers}
//
//class SettingsTest extends FlatSpec with Matchers {
//
//  it should "throw an exception when there is required params missing" in {
//    val cmdLine = Seq[String](
//      "dev",
//      "-kafka-topics", "dummy,dummy2"
//    )
//
//    assertThrows[IllegalArgumentException](Settings.load(cmdLine.toArray))
//  }
//
//  it should "read all mandatory parameters" in {
//    val kafkaTopics = "dummy,dummy2"
//    val yarnMode = "yarn-mode"
//    val kafkaBrokers = "broker1"
//    val dataDir = "/tmp/data"
//    val tempDir = "/tmp/tmp"
//
//    val cmdLine = Seq[String](
//      "dev",
//      "-kafka-topics", kafkaTopics,
//      "-yarn-mode", yarnMode,
//      "-kafka-brokers", kafkaBrokers,
//      "-data-dir", dataDir,
//      "-temp-dir", tempDir
//    )
//    Settings.load(cmdLine.toArray)
//    yarnMode shouldEqual Settings.yarnMode
//    kafkaTopics shouldEqual Settings.kafkaTopics
//    kafkaBrokers shouldEqual Settings.kafkaBrokers
//    dataDir shouldEqual Settings.outputDirectory
//    tempDir shouldEqual Settings.tempDirectory
//  }
//}
