package br.com.ifood.data.featurestore.aggregation.offline

import br.com.ifood.data.featurestore.aggregation.AggregatorBase
import br.com.ifood.data.featurestore.aggregation.config.Settings
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class AggregatorProcessor(spark: SparkSession, bDate: LocalDateTime) extends AggregatorBase {

  override var baseDate: Option[LocalDateTime] = Some(bDate)

  def parseBaseDate(df: DataFrame, baseDate: LocalDateTime): DataFrame = {
    df.withColumn("year", lit(baseDate.getYear))
      .withColumn("month", lit(baseDate.getDayOfMonth))
      .withColumn("day", lit(baseDate.getDayOfMonth))
  }

  override def run(df: DataFrame, joinField: Option[String] = None): DataFrame = {
    require(aggActions.nonEmpty, "You must pass at least one aggregation. Use `addAggAction` or `addCustomAggAction`")

    val result = parseBaseDate(df.groupBy(Settings.groupByField)
      .agg(aggActions.head, aggActions.tail: _*), baseDate.get)

    joinField match {
      case Some(field) => joinColumnsIntoMap(result, field, Seq(Settings.groupByField) ++ dateFields)
      case _ => result
    }

  }

}
