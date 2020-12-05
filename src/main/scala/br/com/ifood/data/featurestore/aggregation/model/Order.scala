package br.com.ifood.data.featurestore.aggregation.model

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

case class Order(key: String,
                 totalAmount: Double,
                 orderCreatedAt: Timestamp,
                 fs_ingestion_timestamp: Timestamp,
                 fs_year: Int,
                 fs_month: Int,
                 fs_day: Int,
                 fs_hour: Int) {
}
