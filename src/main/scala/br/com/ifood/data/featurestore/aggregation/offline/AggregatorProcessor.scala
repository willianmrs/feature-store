package br.com.ifood.data.featurestore.aggregation.offline

import br.com.ifood.data.featurestore.aggregation.config.Settings
import br.com.ifood.data.featurestore.aggregation.model.{AggAction, AggCustomAction, Operation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

import java.sql.Timestamp
import java.time.LocalDateTime

class AggregatorProcessor(spark: SparkSession, baseDate: LocalDateTime) {
  var aggActions: Seq[Column] = Seq.empty
  val dateFields = Seq("year", "month", "day")

  def joinColumnsIntoMap(df: DataFrame, mapColumnName: String, except: Seq[String]): DataFrame = {
    val kvCols = df.columns.filterNot(except.contains(_)).flatMap(c => Seq(lit(c), col(c)))
    val allColumns = except :+ mapColumnName
    df.withColumn(mapColumnName, functions.map(kvCols: _*))
      .select(allColumns.head, allColumns.tail: _*)
  }


  def addAggAction(actions: AggAction*): AggregatorProcessor = {
    aggActions = aggActions ++ actions.map(a => {
      Operation.getSparkExpr(
        a.operation,
        when(to_date(col(Settings.timeField)) > Timestamp.valueOf(baseDate.minusDays(a.period.toDays)), col(a.field)).otherwise(0.0))
        .as(a.featureName)
    })
    this
  }

  def addCustomAction(actions: AggCustomAction*): AggregatorProcessor = {
    aggActions = aggActions ++ actions.map(action => action.aggregation as action.featureName)
    this
  }

  def parseBaseDate(df: DataFrame, baseDate: LocalDateTime): DataFrame = {
    df.withColumn("year", lit(baseDate.getYear))
      .withColumn("month", lit(baseDate.getDayOfMonth))
      .withColumn("day", lit(baseDate.getDayOfMonth))
  }

  def run(df: DataFrame, joinField: Option[String] = None): DataFrame = {
    require(aggActions.nonEmpty, "You must pass at least one aggregation. Use `addAggAction` or `addCustomAggAction`")

    val result = parseBaseDate(df.groupBy(Settings.groupByField)
      .agg(aggActions.head, aggActions.tail: _*), baseDate)

    joinField match {
      case Some(field) => joinColumnsIntoMap(result, field, Seq(Settings.groupByField) ++ dateFields)
      case _ => result
    }

  }

}
