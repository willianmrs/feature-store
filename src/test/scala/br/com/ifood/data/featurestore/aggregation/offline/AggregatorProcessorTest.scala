package br.com.ifood.data.featurestore.aggregation.offline

import br.com.ifood.data.featurestore.aggregation.config.Settings
import br.com.ifood.data.featurestore.aggregation.model.{AggAction, AggCustomAction, Operation}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions.{col, size, sum, when}
import org.scalatest.flatspec.AnyFlatSpecLike

import java.sql.Timestamp
import java.time.{LocalDate, LocalTime}
import scala.concurrent.duration.Duration

class AggregatorProcessorTest extends AnyFlatSpecLike with DataFrameSuiteBase {

  import spark.implicits._

  it should "test stream" in {
    Settings.load(Array("dev",
      "dev",
      "-time-field", "fs_ingestion_timestamp",
      "-agg-field", "customer_id",
    ))
    val df = Seq(
      ("1", 10.0, "2020-12-05", List("1", "2", "3", "4")),
      ("2", 10.0, "2020-12-04", List("1", "2")),
      ("2", 20.0, "2020-12-05", List("1", "2", "3")),
      ("3", 10.0, "2020-12-01", List("1", "2", "3")),
      ("3", 10.0, "2020-12-03", List("1")),
      ("3", 26.0, "2020-12-05", List("1", "3")),
      ("3", 12.0, "2020-12-05", List("1", "2", "3", "5"))
    ).toDF("customer_id", "order_total_amount", "fs_ingestion_timestamp", "items")

    val yesterday = LocalDate.now.minusDays(1).atTime(LocalTime.MAX)
    val dateFilter = col(Settings.timeField) > Timestamp.valueOf(yesterday.minusDays(3))

    val aggActions = Seq(
      AggAction("total-amount-3d", Duration("3 days"), "order_total_amount", Operation.SUM),
      AggAction("total-avg-3d", Duration("3 days"), "order_total_amount", Operation.AVG),
      AggAction("total-amount-1d", Duration("1 days"), "order_total_amount", Operation.SUM),
    )
    val aggCustomActions = Seq(
      AggCustomAction("avg-items-count", sum(when(dateFilter, size(col("items"))).otherwise(0)))
    )

    val processor = new AggregatorProcessor(spark, yesterday)
      .addAggAction(aggActions: _*)
      .addCustomAction(aggCustomActions: _*)

    processor
      .run(df, Some("features"))
      .show(false)
  }
}