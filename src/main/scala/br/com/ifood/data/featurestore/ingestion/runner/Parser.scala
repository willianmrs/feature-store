package br.com.ifood.data.featurestore.ingestion.runner

import br.com.ifood.data.featurestore.ingestion.model.Event
import org.apache.spark.sql.{DataFrame, Dataset}

trait Parser {
  def parse(df: Dataset[Event]): DataFrame
}
