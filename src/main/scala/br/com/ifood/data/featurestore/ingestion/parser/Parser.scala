package br.com.ifood.data.featurestore.ingestion.parser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

trait Parser {
  def parse(df: DataFrame): DataFrame

  def parseDate(columnName: String)(df: DataFrame): DataFrame = {
    df.withColumn(columnName, to_timestamp(col(columnName), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
      .withColumn("fs_year", year(col(columnName)))
      .withColumn("fs_month", month(col(columnName)))
      .withColumn("fs_day", dayofmonth(col(columnName)))
      .withColumn("fs_hour", hour(col(columnName)))
      .withColumn("fs_minute", minute(col(columnName)))
      .withColumn("fs_ingestion_timestamp", current_timestamp())
  }
}
