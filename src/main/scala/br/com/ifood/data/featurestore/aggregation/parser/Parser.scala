package br.com.ifood.data.featurestore.aggregation.parser

import org.apache.spark.sql.DataFrame

trait Parser {
  def parse: DataFrame
}
