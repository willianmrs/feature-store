package br.com.ifood.data.featurestore.aggregation.pipeline

import br.com.ifood.data.featurestore.aggregation.config.Settings
import br.com.ifood.data.featurestore.aggregation.model.{FeatureStoreTable, Order}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ParserOrder(df: DataFrame, spark: SparkSession) extends Base with Serializable {

  import spark.implicits._

  def updateFeatures(fs: FeatureStoreTable, event: Order): FeatureStoreTable = {

    val features = Map(
      "order-total-order-count-1d" -> {
        fs.features.getOrElse("order-total-order-count-1d", 0.0) + 1.0
      },
      "order-total-order-sum-1d" -> {
        fs.features.getOrElse("order-total-order-sum-1d", 0.0) + event.totalAmount
      },
    )
    fs.copy(features = features)
  }

  def CalculateAggregations(id: (String, Int, Int, Int), newEvents: Seq[Order], features: FeatureStoreTable): FeatureStoreTable = {
    newEvents.foldLeft(features)((f, event) => {
      updateFeatures(f, event)
    })
  }

  def calculateFeatures(id: (String, Int, Int, Int), events: Iterator[Order], oldState: GroupState[FeatureStoreTable]): Iterator[FeatureStoreTable] = {
    var output: Iterator[FeatureStoreTable] = Iterator()
    val newEvents = events.toSeq
    val (identifier, year, month, day) = id
    lazy val emptyTable = FeatureStoreTable(identifier, year, month, day, 0, Map())

    println(s"Customer ID: $id")
    println(s"Input Data (${newEvents.size}):")
    println(s"State: $oldState")


    if (oldState.exists && events.isEmpty) {
      if (oldState.hasTimedOut) {
        oldState.remove()
        return Iterator.empty
      } else {
        val featureTable = oldState.get
        val result = CalculateAggregations(id, newEvents, featureTable)
        oldState.update(result)
        output = Iterator(result)
      }
    } else {
      oldState.update(CalculateAggregations(id, newEvents, emptyTable))
    }
    oldState.setTimeoutDuration("1 hour")
    output
  }

  def parseDate(df: DataFrame, columnName: String): DataFrame = {
    df.withColumn(columnName, to_timestamp(col(columnName), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
      .withColumn("fs_year", year(col(columnName)))
      .withColumn("fs_month", month(col(columnName)))
      .withColumn("fs_day", dayofmonth(col(columnName)))
      .withColumn("fs_hour", hour(col(columnName)))
    //      .withColumn("fs_minute", minute(col(columnName)))
    //      .withColumn("fs_ingestion_timestamp", current_timestamp())
  }

  def parse: DataFrame = {
    var result = df.where(col("customer_id").isNotNull)
      .select(
        col("customer_id") as "key",
        col("order_total_amount") as "totalAmount",
        col("order_created_at") as "orderCreatedAt",
        col("fs_ingestion_timestamp") as "fs_ingestion_timestamp"
      )

    result = parseDate(result, "fs_ingestion_timestamp")

    result = result
      .as[Order]
      .groupByKey(event => (event.key, event.fs_year, event.fs_month, event.fs_day))
      .flatMapGroupsWithState(
        outputMode = OutputMode.Update(),
        timeoutConf = GroupStateTimeout.ProcessingTimeTimeout())(calculateFeatures).toDF
    result
  }

}
