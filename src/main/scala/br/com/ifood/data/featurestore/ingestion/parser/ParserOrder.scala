package br.com.ifood.data.featurestore.ingestion.parser

import br.com.ifood.data.featurestore.ingestion.schemas.OrderSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class ParserOrder(spark: SparkSession) extends Parser {

  import spark.implicits._

  def parseEvent()(df: DataFrame): DataFrame = {
    df.withColumn("data", from_json('value, OrderSchema.getSchema))
      .select('key, col("data.*"))
      .withColumn("items", from_json('items, OrderSchema.items))
  }

  def parse(df: DataFrame): DataFrame = {
    val pipeline = parseEvent() _ andThen parseDate("order_created_at")
    pipeline(df.toDF)
  }
}

object ParserOrder {
  def apply(df: DataFrame, spark: SparkSession): Parser = {
    new ParserOrder(spark)
  }
}