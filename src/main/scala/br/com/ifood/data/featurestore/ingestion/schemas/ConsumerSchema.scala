package br.com.ifood.data.featurestore.ingestion.schemas

import org.apache.spark.sql.types._

object ConsumerSchema {
  def getSchema = new StructType(
    Array(
      StructField("active", StringType, nullable = false),
      StructField("created_at", StringType, nullable = false),
      StructField("customer_id", StringType, nullable = false),
      StructField("customer_name", StringType, nullable = false),
      StructField("customer_phone_area", StringType, nullable = false),
      StructField("customer_phone_number", StringType, nullable = false),
      StructField("language", StringType, nullable = false)
    )
  )
}






