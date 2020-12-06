package br.com.ifood.data.featurestore.aggregation

import br.com.ifood.data.featurestore.aggregation.config.Settings
import br.com.ifood.data.featurestore.aggregation.model.{AggAction, Operation}
import br.com.ifood.data.featurestore.aggregation.offline.AggregatorProcessor
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.time.{LocalDate, LocalDateTime, LocalTime}
import scala.concurrent.duration.Duration

object AggregationsOfflineMain {
  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Settings.load(Array("dev",
      "dev",
      "-master-mode", "local[*]",
      "-input-data-table", "/tmp/ifood/data/ingestion/order-events/",
      "-output-data-table", "/tmp/ifood/data/aggregations/offline/order-agg",
      "-temp-dir", "tempDir",
      "-time-field", "fs_ingestion_timestamp",
      "-group-field", "customer_id",
    ))

    logger.info(s"JobName: ${Settings} started at: ${LocalDateTime.now}")

    val spark = SparkSession
      .builder()
      .appName(Settings.appName)
      .master(Settings.masterMode)
      .config(new SparkConf()
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      )
      .getOrCreate()
    val df = spark.read.format("delta").load("/tmp/ifood/data/ingestion/order-events")
    val actions = Seq(
      AggAction("total-amount-3d", Duration("3 days"), "order_total_amount", Operation.SUM),
      AggAction("total-avg-3d", Duration("3 days"), "order_total_amount", Operation.AVG),
      AggAction("total-amount-1d", Duration("1 days"), "order_total_amount", Operation.SUM),
    )

    val yesterday = LocalDate.now.minusDays(1).atTime(LocalTime.MAX)

    val processor = new AggregatorProcessor(spark, yesterday)

    processor.addAggAction(actions: _*).run(df)
      .write.format("delta")
      .partitionBy("year", "month", "day")
      .save(Settings.outputTable)
  }
}
