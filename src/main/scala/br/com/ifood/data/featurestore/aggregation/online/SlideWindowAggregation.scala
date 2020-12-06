package br.com.ifood.data.featurestore.aggregation.online

import br.com.ifood.data.featurestore.aggregation.config.Settings
import br.com.ifood.data.featurestore.aggregation.model.AggCustomAction
import org.apache.spark.sql.functions.{col, expr, lit, window}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

class SlideWindowAggregation(spark: SparkSession) {
  //  TODO: Import those aggragations from a configuration file.
  def getAllExpr: List[AggCustomAction] = {
    List(
      AggCustomAction("customer-count-1h", expr("COUNT(customer_id)")),
      AggCustomAction("total-amount-sum-1h", expr("SUM(CAST(`order_total_amount` as Double))")),
      AggCustomAction("total-amount-mean-1h", expr("MEAN(CAST(`order_total_amount` as Double))")),
      AggCustomAction("total-amount-min-1h", expr("MIN(CAST(`order_total_amount` as Double))")),
      AggCustomAction("total-amount-max-1h", expr("MAX(CAST(`order_total_amount` as Double))")),
      AggCustomAction("total-amount-avg-1h", expr("AVG(CAST(`order_total_amount` as Double))")),
    )
  }

  def joinColumnsIntoMap(df: DataFrame, mapColumnName: String, except: List[String]): DataFrame = {
    val kvCols = df.columns.filterNot(except.contains(_)).flatMap(c => Seq(lit(c), col(c)))
    val allColumns = except :+ mapColumnName
    df.withColumn(mapColumnName, functions.map(kvCols: _*))
      .select(allColumns.head, allColumns.tail: _*)
  }

  def agg(df: DataFrame): DataFrame = {
    val aggregations = getAllExpr.map(agg => agg.aggregation.as(agg.featureName))
    val agg = df.groupBy(
      col(Settings.groupByField),
      window(col(Settings.timeField), Settings.windowDuration, Settings.windowSlideDuration)
    )
      .agg(aggregations.head, aggregations.tail: _*)

    joinColumnsIntoMap(agg, "features", List(Settings.groupByField, "window"))

  }
}
