package br.com.ifood.data.featurestore.aggregation

import br.com.ifood.data.featurestore.aggregation.config.Settings
import br.com.ifood.data.featurestore.aggregation.model.{AggAction, AggCustomAction, Operation}
import org.apache.spark.sql.functions.{col, lit, to_date, when}
import org.apache.spark.sql.{Column, DataFrame, functions}

import java.sql.Timestamp
import java.time.LocalDateTime

trait AggregatorBase {
  var aggActions: Seq[Column] = Seq.empty
  val dateFields = Seq("year", "month", "day")
  var baseDate: Option[LocalDateTime]

  def run(df: DataFrame, joinField: Option[String] = None): DataFrame

  def joinColumnsIntoMap(df: DataFrame, mapColumnName: String, except: Seq[String]): DataFrame = {
    val kvCols = df.columns.filterNot(except.contains(_)).flatMap(c => Seq(lit(c), col(c)))
    val allColumns = except :+ mapColumnName
    df.withColumn(mapColumnName, functions.map(kvCols: _*))
      .select(allColumns.head, allColumns.tail: _*)
  }

  def addAggAction(actions: AggAction*): AggregatorBase = {
    baseDate match {
      case Some(date) =>
        aggActions = aggActions ++ actions.map(a => {
          Operation.getSparkExpr(
            a.operation,
            when(to_date(col(Settings.timeField)) > Timestamp.valueOf(date.minusDays(a.period.toDays)), col(a.field)).otherwise(0.0))
            .as(a.featureName)
        })
      case None => aggActions = aggActions ++ actions.map(a => {
        Operation.getSparkExpr(
          a.operation,
          col(a.field))
          .as(a.featureName)
      })
    }
    this
  }

  def addCustomAction(actions: AggCustomAction*): AggregatorBase = {
    aggActions = aggActions ++ actions.map(action => action.aggregation as action.featureName)
    this
  }
}
