package br.com.ifood.data.featurestore.ingestion.parser

import br.com.ifood.data.featurestore.ingestion.schemas.RestaurantSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class ParserRestaurant(spark: SparkSession) extends Parser {

  import spark.implicits._

  def parseEvent()(df: DataFrame): DataFrame = {
    df.withColumn("data", from_json('value, RestaurantSchema.getSchema))
      .select('key, col("data.*"))
  }

  def parse(df: DataFrame): DataFrame = {
    val pipeline = parseEvent() _ andThen parseDate("created_at")
    pipeline(df.toDF)
  }
}

