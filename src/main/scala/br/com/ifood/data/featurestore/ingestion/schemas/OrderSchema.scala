package br.com.ifood.data.featurestore.ingestion.schemas

import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType}

object OrderSchema {
  def valueType = new StructType().add(StructField("value", StringType)).add(StructField("currency", StringType))

  def garnishItems = ArrayType(new StructType(
    Array(
      StructField("name", StringType, nullable = true),
      StructField("addition", valueType, nullable = true),
      StructField("discount", valueType, nullable = true),
      StructField("quantity", StringType, nullable = true),
      StructField("sequence", StringType, nullable = true),
      StructField("unitPrice", valueType, nullable = true),
      StructField("categoryId", StringType, nullable = true),
      StructField("externalId", StringType, nullable = true),
      StructField("totalValue", valueType, nullable = true),
      StructField("categoryName", StringType, nullable = true),
      StructField("integrationId", StringType, nullable = true)
    )
  ))

  def items = ArrayType(new StructType(
    Array(
      StructField("name", StringType, nullable = true),
      StructField("addition", valueType, nullable = true),
      StructField("discount", valueType, nullable = true),
      StructField("quantity", StringType, nullable = true),
      StructField("sequence", StringType, nullable = true),
      StructField("unitPrice", valueType, nullable = true),
      StructField("externalId", StringType, nullable = true),
      StructField("totalValue", valueType, nullable = true),
      StructField("customerNote", StringType, nullable = true),
      StructField("garnishItems", garnishItems, nullable = true),
      StructField("integrationId", StringType, nullable = true),
      StructField("totalAddition", valueType, nullable = true),
      StructField("totalDiscount", valueType, nullable = true),
    )
  ))

  def getSchema = new StructType(
    Array(
      StructField("customer_id", StringType, nullable = true),
      StructField("cpf", StringType, nullable = true),
      StructField("customer_name", StringType, nullable = true),
      StructField("delivery_address_city", StringType, nullable = true),
      StructField("delivery_address_country", StringType, nullable = true),
      StructField("delivery_address_district", StringType, nullable = true),
      StructField("delivery_address_external_id", StringType, nullable = true),
      StructField("delivery_address_latitude", StringType, nullable = true),
      StructField("delivery_address_longitude", StringType, nullable = true),
      StructField("delivery_address_state", StringType, nullable = true),
      StructField("delivery_address_zip_code", StringType, nullable = true),
      StructField("items", StringType, nullable = true),
      StructField("merchant_id", StringType, nullable = true),
      StructField("merchant_latitude", StringType, nullable = true),
      StructField("merchant_longitude", StringType, nullable = true),
      StructField("merchant_timezone", StringType, nullable = true),
      StructField("order_created_at", StringType, nullable = true),
      StructField("order_id", StringType, nullable = true),
      StructField("order_scheduled", StringType, nullable = true),
      StructField("order_total_amount", DoubleType, nullable = true),
      StructField("origin_platform", StringType, nullable = true),
    )
  )
}
