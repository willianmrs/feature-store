package br.com.ifood.data.featurestore.ingestion.schemas

import org.apache.spark.sql.types._

object OrderStatusSchema {
  def getSchema = new StructType(
    Array(
      StructField("created_at", StringType, nullable = true),
      StructField("order_id", StringType, nullable = true),
      StructField("status_id", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    )
  )
}
