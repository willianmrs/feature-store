package br.com.ifood.data.featurestore.aggregation.slidewindow

import java.time.LocalDateTime

import br.com.ifood.data.featurestore.aggregation.config.Settings
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SlideWindowAggregationsMain {
  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Settings.load(Array("dev",
      "dev",
      "-yarn-mode", "local[*]",
      "-input-data-table", "/tmp/ifood/data/ingestion/order-events/",
      "-output-data-table", "/tmp/ifood/data/aggregations/slide-window/order-agg",
      "-temp-dir", "tempDir",
      "-window-duration", "5 seconds",
      "-window-slide-duration", "5 seconds",
      "-watermark", "10 seconds",
      "-time-field", "fs_ingestion_timestamp",
      "-agg-field", "customer_id",
    ))
    Settings.outputTable

    logger.info(s"JobName: ${Settings} started at: ${LocalDateTime.now}")

    val spark = SparkSession
      .builder()
      .appName(Settings.appName)
      .master(Settings.yarnMode)
      .config(new SparkConf()
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      )
      .getOrCreate()


    val df = spark.readStream
      .format("delta")
      .option("ignoreDeletes", "true")
      .load(s"${Settings.inputTable}")
      .withWatermark(Settings.timeField, Settings.watermark)

    val agg = new SlideWindowAggregation(spark).agg(df)

    agg
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("mergeSchema", "true")
      .option("checkpointLocation", s"/tmp/ifood/metadata/aggregations/_checkpoints/${Settings.outputTable}")
      .start(Settings.outputTable)
      .awaitTermination()
  }

}
