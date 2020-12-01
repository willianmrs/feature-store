package br.com.ifood.data.featurestore.aggregation.model

case class FeatureStoreTable(key: String,
                             fs_year: Int,
                             fs_month: Int,
                             fs_day: Int,
                             fs_hour: Int,
                             features: Map[String, String]) {

  def update(f: FeatureStoreTable): FeatureStoreTable = {
    FeatureStoreTable(this.key, this.fs_year, this.fs_month, this.fs_hour, this.fs_hour, this.features ++ f.features)
  }
}

