package br.com.ifood.data.featurestore.aggregation.model

case class DatasetConf(inputPath: String, partitionColumns: List[String], datasetTableName: String)
