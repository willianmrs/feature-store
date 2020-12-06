package br.com.ifood.data.featurestore.publisher

import org.apache.spark.sql.DataFrame

trait Publisher {
  def process(df: DataFrame): DataFrame

  def read(): DataFrame

  def save(df: DataFrame): Unit
}
