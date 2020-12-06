package br.com.ifood.data.featurestore.aggregation.model

import org.apache.spark.sql.Column

case class AggCustomAction(featureName: String,
                           aggregation: Column)
