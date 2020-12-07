package br.com.ifood.data.featurestore.publisher

import br.com.ifood.data.featurestore.publisher.config.Settings
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

class MysqlPublisher(spark: SparkSession) extends Publisher {
  override var dataFrame: DataFrame = _

  override def process(df: DataFrame): Publisher = {
    dataFrame = df
    this
  }

  override def save(): Unit = {
    val df = spark.table("...")
    val jdbcUrl = s"jdbc:mysql://${Settings.jdbcHostname}:${Settings.jdbcPort}/${Settings.jdbcDatabase}"
    df.coalesce(10).write.mode("append").jdbc(jdbcUrl, "product_mysql", new Properties())
  }
}
