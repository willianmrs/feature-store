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
    val jdbcUrl = s"jdbc:mysql://${Settings.jdbcHostname}:${Settings.jdbcPort}/${Settings.jdbcDatabase}"
    val properties = new Properties()
    //properties.put
    properties.put("password", "root")
    properties.put("user", "root")
    dataFrame.write
      .mode("overwrite") // <--- Overwrite the existing table
      .jdbc(jdbcUrl, "order_agg", properties)

  }
}
