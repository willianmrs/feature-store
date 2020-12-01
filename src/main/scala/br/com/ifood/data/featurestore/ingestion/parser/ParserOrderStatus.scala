package br.com.ifood.data.featurestore.ingestion.parser

import br.com.ifood.data.featurestore.ingestion.schemas.OrderStatusSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class ParserOrderStatus(spark: SparkSession) extends Parser {

  import spark.implicits._

  def parseEvent()(df: DataFrame): DataFrame = {
    df.withColumn("data", from_json('value, OrderStatusSchema.getSchema))
      .select('key, col("data.*"))
  }

  def parse(df: DataFrame): DataFrame = {
    val pipeline = parseEvent() _ andThen parseDate("created_at")
    pipeline(df.toDF)
  }
}

object ParserOrderStatus {
  def apply(df: DataFrame, spark: SparkSession): Parser = {
    new ParserOrder(spark)
  }
}