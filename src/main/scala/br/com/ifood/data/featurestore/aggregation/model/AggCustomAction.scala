package br.com.ifood.data.featurestore.aggregation.model

import org.apache.spark.sql.Column

import scala.concurrent.duration.Duration

case class AggCustomAction(featureName: String,
                           aggregation: Column)
