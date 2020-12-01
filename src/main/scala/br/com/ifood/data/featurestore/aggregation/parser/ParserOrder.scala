package br.com.ifood.data.featurestore.aggregation.parser

import br.com.ifood.data.featurestore.aggregation.model.{FeatureStoreTable, Order}
import org.apache.spark.sql.functions.{col, current_timestamp}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ParserOrder(df: DataFrame, spark: SparkSession) extends Parser {

  import spark.implicits._

  def CalculateAggregations(id: (String, Int, Int, Int), newEvents: Seq[Order], events: List[Order]): (FeatureStoreTable, List[Order]) = {
    val allEvents = newEvents ++ events
    Map()
    val features = Map(
      "total-order-total-1h" -> allEvents.map(_.totalAmount).sum.toString,
      "total-order-count-1h" -> allEvents.length.toString,
      "total-order-min-1h" -> allEvents.map(_.totalAmount).min.toString,
      "total-order-max-1h" -> allEvents.map(_.totalAmount).max.toString,
      "total-order-mean-1h" -> (allEvents.map(_.totalAmount).sum / allEvents.map(_.totalAmount).sum).toString,
    )

    val event = newEvents.head
    val featureStore = FeatureStoreTable(id._1, event.fs_year, event.fs_month, event.fs_day, event.fs_hour, features)

    (featureStore, events)
  }

  def calculateFeatures(id: (String, Int, Int, Int), events: Iterator[Order], state: GroupState[List[Order]]): Iterator[FeatureStoreTable] = {

    val newEvents = events.toSeq
    if (newEvents.isEmpty) return Iterator.empty
    println(s"Customer ID: $id")
    println(s"Input Data (${newEvents.size}):")
    println(s"State: $state")
    val initialState = List[Order]()
    val oldState = state.getOption.getOrElse(initialState)
    val (featureTable, newState) = CalculateAggregations(id, newEvents, oldState)
    state.update(newState)
    Iterator(featureTable)
  }


  override def parse: DataFrame = {
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
