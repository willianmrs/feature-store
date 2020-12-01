package br.com.ifood.data.featurestore.ingestion.runner

import br.com.ifood.data.featurestore.ingestion.config.Settings
import br.com.ifood.data.featurestore.ingestion.model.Event
import br.com.ifood.data.featurestore.ingestion.parser.ParserFactory
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class Runner(df: DataFrame, spark: SparkSession) {
  def start(): DataFrame = {
    val parser = ParserFactory(Settings.streamType, spark)
    parser.parse(df)
  }

}

object Runner {
  def apply(df: DataFrame, spark: SparkSession): Runner = {
    new Runner(df, spark)
  }
}