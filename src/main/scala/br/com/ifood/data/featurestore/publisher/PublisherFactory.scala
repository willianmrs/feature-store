package br.com.ifood.data.featurestore.publisher

import org.apache.spark.sql.SparkSession

object PublisherFactory {
  def apply(publisherType: String, spark: SparkSession): Publisher = {
    publisherType match {
      case "historical" => new HistoricalPubisher(spark)
      case "mysql" => new MysqlPublisher(spark)
      case t => throw new NotImplementedError(s"Publisher type $t not implemented.")
    }
  }

}
