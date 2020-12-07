package br.com.ifood.data.featurestore.aggregation.online

import br.com.ifood.data.featurestore.aggregation.AggregatorBase
import br.com.ifood.data.featurestore.aggregation.config.Settings
import org.apache.spark.sql.functions.{col, window}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class SlideWindowAggregation(spark: SparkSession) extends AggregatorBase {
  override var baseDate: Option[LocalDateTime] = _

  override def run(df: DataFrame, joinField: Option[String]): DataFrame = {
    require(aggActions.nonEmpty, "You must pass at least one aggregation. Use `addAggAction` or `addCustomAggAction`")

    val agg = df.groupBy(
      col(Settings.groupByField),
      window(col(Settings.timeField), Settings.windowDuration, Settings.windowSlideDuration)
    )
      .agg(aggActions.head, aggActions.tail: _*)

    joinColumnsIntoMap(agg, "features", List(Settings.groupByField, "window"))

  }
}
