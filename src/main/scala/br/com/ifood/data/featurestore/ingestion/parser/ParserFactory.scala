package br.com.ifood.data.featurestore.ingestion.parser

import org.apache.spark.sql.SparkSession

object ParserFactory {
  def apply(parserType: String, spark: SparkSession): Parser = {
    parserType match {
      case "order-events" => new ParserOrder(spark)
      case "order-status-events" => new ParserOrderStatus(spark)
      case "restaurant-events" => new ParserRestaurant(spark)
      case "consumer-events" => new ParserConsummer(spark)
      case r => throw new NotImplementedError(s"Runner type $r not implemented.")
    }
  }
}
