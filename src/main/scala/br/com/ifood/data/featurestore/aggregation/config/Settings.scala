package br.com.ifood.data.featurestore.aggregation.config

import org.apache.commons.cli.{BasicParser, CommandLine, Options}

object Settings {
  private var settings: Option[CommandLine] = None
  private var environment: String = _

  def env = environment

  def appName = settings.get.getOptionValue("app-name", "feature-store-data-ingest")

  def yarnMode = settings.get.getOptionValue("yarn-mode")

  def inputTable = settings.get.getOptionValue("input-data-table")

  def outputTable = settings.get.getOptionValue("output-data-table")

  def windowDuration = settings.get.getOptionValue("window-duration")

  def windowSlideDuration = settings.get.getOptionValue("window-slide-duration")

  def watermark = settings.get.getOptionValue("watermark")

  def timeField = settings.get.getOptionValue("time-field")

  def aggField = settings.get.getOptionValue("agg-field")

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
      .addOption("k", "kafka-topics", true, "Kafka topics")
      .addOption("y", "yarn-mode", true, "Yarn mode")
      .addOption("b", "kafka-brokers", true, "Kafka brokers.")
      .addOption("m", "kafka-max-offsets-per-trigger", true, "Rate limit on maximum number of offsets processed per trigger interval")
      .addOption("it", "input-data-table", true, "Input data table")
      .addOption("ot", "output-data-table", true, "Output data table")
      .addOption("t", "temp-dir", true, "Temporary directory")
      .addOption("wd", "window-duration", true, "window-duration")
      .addOption("wsd", "window-slide-duration", true, "window-slide-duration")
      .addOption("w", "watermark", true, "watermark")
      .addOption("tf", "time-field", true, "time field")
      .addOption("af", "agg-field", true, "Aggregation Field")

    val requiredOpts = Seq(

    )

    environment = args.head
    settings = validateLoadedParams(options, requiredOpts, args.tail)
  }

}
