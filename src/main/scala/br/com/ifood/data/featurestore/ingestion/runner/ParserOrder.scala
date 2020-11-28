package br.com.ifood.data.featurestore.ingestion.runner

import br.com.ifood.data.featurestore.ingestion.model.Event
import br.com.ifood.data.featurestore.ingestion.schemas.OrderSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class ParserOrder(spark: SparkSession) extends Parser {

  import spark.implicits._

  def parse(df: Dataset[Event]): DataFrame = {
    df.select('value)
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .withColumn("data", from_json('value, OrderSchema.getSchema))
      .select('key, col("data.*"))
      .withColumn("items", from_json('items, OrderSchema.items))
      .toDF()
  }
}

object ParserOrder {
  def apply(df: DataFrame, spark: SparkSession): Parser = {
    new ParserOrder(spark)
  }
}