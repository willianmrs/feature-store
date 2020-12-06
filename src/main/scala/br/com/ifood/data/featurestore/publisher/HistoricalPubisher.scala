package br.com.ifood.data.featurestore.publisher

import br.com.ifood.data.featurestore.publisher.config.Settings
import org.apache.spark.sql.{DataFrame, SparkSession}

class HistoricalPubisher(spark: SparkSession) extends Publisher {
  override def process(df: DataFrame): DataFrame = {
    df
  }

  override def read(): DataFrame = {
    spark.read.format("delta").load(Settings.inputDirectory)
  }

  override def save(df: DataFrame): Unit = {
    df.write.format("delta").save(Settings.outputDirectory)
  }
}
