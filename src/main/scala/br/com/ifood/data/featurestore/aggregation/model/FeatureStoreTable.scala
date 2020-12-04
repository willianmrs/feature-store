package br.com.ifood.data.featurestore.aggregation.model

case class FeatureStoreTable(key: String,
                             fs_year: Int,
                             fs_month: Int,
                             fs_day: Int,
                             fs_hour: Int,
                             features: Map[String, Double]) {

}

