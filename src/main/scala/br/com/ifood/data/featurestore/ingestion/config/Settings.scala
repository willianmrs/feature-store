package br.com.ifood.data.featurestore.ingestion.config

import org.apache.commons.cli.{BasicParser, CommandLine, Options}

object Settings {
  private var settings: Option[CommandLine] = None
  private var environment: String = _

  def env: String = environment

  def appName: String = settings.get.getOptionValue("app-name", "feature-store-data-ingest")

  def kafkaTopics: String = settings.get.getOptionValue("kafka-topics")

  def masterMode: String = settings.get.getOptionValue("master-mode")

  def kafkaBrokers: String = settings.get.getOptionValue("kafka-brokers")

  def kafkaMaxOffsetsPerTrigger: Int = settings.get.getOptionValue("kafka-max-offsets-per-trigger", "100").toInt

  def outputDirectory: String = settings.get.getOptionValue("output-dir")

  def streamType: String = settings.get.getOptionValue("stream-type", "order")
  def maxOffsetsPerTrigger: Long = settings.get.getOptionValue("max-offsets-per-trigger", "order").toLong

  def validateLoadedParams(options: Options, mandatoryParams: Seq[String], tailArgs: Array[String]): Option[CommandLine] = {
    val parsed = new BasicParser().parse(options, tailArgs)
    mandatoryParams.foreach(mandatoryParam => {
      if (parsed.getOptionValue(mandatoryParam) == null) {
        throw new IllegalArgumentException(s"The argument ${mandatoryParam} is mandatory")
      }
    })
    Option(parsed)
  }


  def load(args: Array[String]): Unit = {
    require(args.length > 0, "Arguments are needed.")

    val options = new Options()
      .addOption("a", "app-name", true, "Define current job's name.")
      .addOption("s", "stream-type", true, "Stream type")
      .addOption("k", "kafka-topics", true, "Kafka topics")
      .addOption("y", "master-mode", true, "Master mode")
      .addOption("b", "kafka-brokers", true, "Kafka brokers.")
      .addOption("m", "kafka-max-offsets-per-trigger", true, "Rate limit on maximum number of offsets processed per trigger interval")
      .addOption("d", "output-dir", true, "Output data directory")
      .addOption("mopt", "max-offsets-per-trigger", true, "Max Offset per Trigger")

    val requiredOpts = Seq(
//      "kafka-topics",
//      "stream-type",
//      "yarn-mode",
//      "kafka-brokers",
//      "data-dir",
//      "temp-dir"
    )

    environment = args.head
    settings = validateLoadedParams(options, requiredOpts, args.tail)
  }

}
