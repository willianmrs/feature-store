package br.com.ifood.data.featurestore.publisher

import br.com.ifood.data.featurestore.publisher.config.Settings
import org.apache.spark.sql.{DataFrame, SparkSession}

class HistoricalPublisher(spark: SparkSession) extends Publisher {
  override var dataFrame: DataFrame = _

  override def process(df: DataFrame): Publisher = {
    dataFrame=df
    this
  }

  override def save(): Unit = {
    dataFrame.write.format("delta").save(Settings.outputDirectory)
  }
}
