package br.com.ifood.data.featurestore.publisher.config

import org.apache.commons.cli.{BasicParser, CommandLine, Options}

object Settings {
  private var settings: Option[CommandLine] = None
  private var environment: String = _

  def env: String = environment

  def appName: String = settings.get.getOptionValue("app-name", "feature-store-data-ingest")

  def masterMode: String = settings.get.getOptionValue("master-mode")

  def inputDirectory: String = settings.get.getOptionValue("input-table")
  def outputDirectory: String = settings.get.getOptionValue("output-table")

  def publisherType: String = settings.get.getOptionValue("publisher-type", "historical")

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
      .addOption("s", "publisher-type", true, "Publisher type")
      .addOption("i", "input-table", true, "Output data directory")
      .addOption("d", "output-table", true, "Output data directory")

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
