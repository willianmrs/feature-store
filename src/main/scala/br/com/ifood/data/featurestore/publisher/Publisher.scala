package br.com.ifood.data.featurestore.publisher

import org.apache.spark.sql.DataFrame

trait Publisher {
  var dataFrame:DataFrame
  def process(df: DataFrame): Publisher

  def save(): Unit
}
