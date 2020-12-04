package br.com.ifood.data.featurestore.aggregation.pipeline

import br.com.ifood.data.featurestore.aggregation.model.{FeatureStoreTable, Order}
import org.apache.spark.sql.functions.{col, current_timestamp}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ParserOrder(df: DataFrame, spark: SparkSession) extends Base with Serializable {

  import spark.implicits._

  def updateFeatures(fs: FeatureStoreTable, event: Order): FeatureStoreTable = {
    val features = Map(
      "order-total-order-count-1d" -> (fs.features("order-total-order-count-1d") + 1),
      "order-total-order-sum-1d" -> (fs.features("order-total-order-sum-1d") + event.totalAmount),
    )
    fs.copy(features = features)
  }

  def CalculateAggregations(id: (String, Int, Int, Int), newEvents: Seq[Order], features: FeatureStoreTable): FeatureStoreTable = {
    newEvents.foldLeft(features)((f, event) => {
      updateFeatures(f, event)
    })
  }

  def calculateFeatures(id: (String, Int, Int, Int), events: Iterator[Order], state: GroupState[FeatureStoreTable]): Iterator[FeatureStoreTable] = {
    val (identifier, year, month, day) = id
    val newEvents = events.toSeq
    if (newEvents.isEmpty) return Iterator.empty
    println(s"Customer ID: $id")
    println(s"Input Data (${newEvents.size}):")
    println(s"State: $state")
    val initialState = FeatureStoreTable(identifier, year, month, day, 0, Map(
      "order-total-order-count-1d" -> 0,
      "order-total-order-sum-1d" -> 0
    ))

    val oldState = state.getOption.getOrElse(initialState)
    val featureTable = CalculateAggregations(id, newEvents, oldState)
    state.update(featureTable)
    Iterator(featureTable)
  }


  def parse: DataFrame = {
    val result = df.withColumn("timestamp", current_timestamp())
      .where(col("customer_id").isNotNull)
      .select(
        col("customer_id") as "key",
        col("order_total_amount") as "totalAmount",
        col("order_created_at") as "orderCreatedAt",
        col("fs_year") as "fs_year",
        col("fs_month") as "fs_month",
        col("fs_day") as "fs_day",
        col("fs_hour") as "fs_hour",
        col("fs_minute") as "fs_minute",
        col("timestamp") as "timestamp"
      )
      .as[Order]
      .groupByKey(event => (event.key, event.fs_year, event.fs_month, event.fs_day))
      .flatMapGroupsWithState(
        outputMode = OutputMode.Append(),
        timeoutConf = GroupStateTimeout.NoTimeout())(calculateFeatures)

    result.toDF
  }

}
