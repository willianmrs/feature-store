package br.com.ifood.data.featurestore.aggregation

import br.com.ifood.data.featurestore.aggregation.config.Settings
import br.com.ifood.data.featurestore.aggregation.online.SlideWindowAggregation
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.time.LocalDateTime

object AggregationsOnlineMain {
  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Settings.load(args)

    logger.info(s"JobName: ${Settings} started at: ${LocalDateTime.now}")

    val spark = SparkSession
      .builder()
      .appName(Settings.appName)
      .master(Settings.masterMode)
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
      .option("checkpointLocation", s"/tmp/ifood/metadata/aggregations/_checkpoints/online/order-agg")
      .start(Settings.outputTable)
      .awaitTermination()
  }

}
