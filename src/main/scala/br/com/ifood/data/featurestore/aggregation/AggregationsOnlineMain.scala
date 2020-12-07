package br.com.ifood.data.featurestore.aggregation

import br.com.ifood.data.featurestore.aggregation.config.Settings
import br.com.ifood.data.featurestore.aggregation.model.AggCustomAction
import br.com.ifood.data.featurestore.aggregation.online.SlideWindowAggregation
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
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

    val aggs = List(
      AggCustomAction("customer-count-1h", expr("COUNT(customer_id)")),
      AggCustomAction("total-amount-sum-1h", expr("SUM(CAST(`order_total_amount` as Double))")),
      AggCustomAction("total-amount-mean-1h", expr("MEAN(CAST(`order_total_amount` as Double))")),
      AggCustomAction("total-amount-min-1h", expr("MIN(CAST(`order_total_amount` as Double))")),
      AggCustomAction("total-amount-max-1h", expr("MAX(CAST(`order_total_amount` as Double))")),
      AggCustomAction("total-amount-avg-1h", expr("AVG(CAST(`order_total_amount` as Double))")),
    )

    val agg = new SlideWindowAggregation(spark).addCustomAction(aggs: _*).run(df)

    agg.writeStream
      .format("delta")
      .outputMode("append")
      .option("mergeSchema", "true")
      .option("checkpointLocation", s"/tmp/ifood/metadata/aggregations/_checkpoints/online/order-agg")
      .start(Settings.outputTable)
      .awaitTermination()
  }

}
