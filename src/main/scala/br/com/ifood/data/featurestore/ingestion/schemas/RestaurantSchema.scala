package br.com.ifood.data.featurestore.ingestion.schemas

import org.apache.spark.sql.types._

object RestaurantSchema {
  def getSchema = new StructType(
    Array(
      StructField("average_ticket", StringType, nullable = true),
      StructField("created_at", StringType, nullable = true),
      StructField("delivery_time", StringType, nullable = true),
      StructField("enabled", StringType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("merchant_city", StringType, nullable = true),
      StructField("merchant_country", StringType, nullable = true),
      StructField("merchant_state", StringType, nullable = true),
      StructField("merchant_zip_code", StringType, nullable = true),
      StructField("minimum_order_value", StringType, nullable = true),
      StructField("price_range", StringType, nullable = true),
      StructField("takeout_time", StringType, nullable = true),
    )
  )
}
