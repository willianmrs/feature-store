package br.com.ifood.data.featurestore.aggregation.slidewindow

import br.com.ifood.data.featurestore.aggregation.config.Settings
import org.apache.spark.sql.functions.{col, expr, lit, window}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

class SlideWindowAggregation(spark: SparkSession) {
  //  TODO: Import those aggragations from a configuration file.
  def getAllExpr: List[String] = {
    List(
      "COUNT(customer_id) AS `customer-count-1h`",
      "SUM(CAST(`order_total_amount` as Double)) AS `total-amount-sum-1h`",
      "MEAN(CAST(`order_total_amount` as Double)) AS `total-amount-mean-1h`",
      "MIN(CAST(`order_total_amount` as Double)) AS `total-amount-min-1h`",
      "MAX(CAST(`order_total_amount` as Double)) AS `total-amount-max-1h`",
      "AVG(CAST(`order_total_amount` as Double)) AS `total-amount-avg-1h`",
    )
  }

  def joinColumnsIntoMap(df: DataFrame, mapColumnName: String, except: List[String]): DataFrame = {
    val kvCols = df.columns.filterNot(except.contains(_)).flatMap(c => Seq(lit(c), col(c)))
    val allColumns = except :+ mapColumnName
    df.withColumn(mapColumnName, functions.map(kvCols: _*))
      .select(allColumns.head, allColumns.tail: _*)
  }

  def agg(df: DataFrame): DataFrame = {
    val aggregations = getAllExpr.map(expr)
    val agg = df.groupBy(
      col(Settings.aggField),
      window(col(Settings.timeField), Settings.windowDuration, Settings.windowSlideDuration)
    )
      .agg(aggregations.head, aggregations.tail: _*)

    joinColumnsIntoMap(agg, "features", List(Settings.aggField, "window"))

  }
}
