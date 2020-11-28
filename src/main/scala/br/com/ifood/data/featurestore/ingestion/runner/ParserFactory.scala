package br.com.ifood.data.featurestore.ingestion.runner

import org.apache.spark.sql.SparkSession

object ParserFactory {
  def apply(parserType: String, spark: SparkSession): Parser = {
    parserType match {
      case "order" => new ParserOrder(spark)
      case r => throw new NotImplementedError(s"Runner type $r not implemented.")
    }
  }
}



