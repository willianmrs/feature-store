package br.com.ifood.data.featurestore.aggregation

import br.com.ifood.data.featurestore.aggregation.config.Settings
import br.com.ifood.data.featurestore.aggregation.model.{AggAction, Operation}
import br.com.ifood.data.featurestore.aggregation.offline.AggregatorProcessor
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime, LocalTime}
import scala.concurrent.duration.Duration

object AggregationsOfflineMain {
  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Settings.load(args)
    val startDate = LocalDate.parse(Settings.startDate).atTime(LocalTime.MAX)
    val endDate = LocalDate.parse(Settings.endDate).atTime(LocalTime.MAX)

    logger.info(s"JobName: ${Settings} started at: ${LocalDateTime.now}")

    val spark = SparkSession
      .builder()
      .appName(Settings.appName)
      .master(Settings.masterMode)
      .config(new SparkConf()
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      )
      .getOrCreate()
    val df = spark.read
      .format("delta")
      .load(Settings.inputTable)
      .filter(col(Settings.timeField) > Timestamp.valueOf(startDate))

    val actions = Seq(
      AggAction("total-amount-3d", Duration("3 days"), "order_total_amount", Operation.SUM),
      AggAction("total-avg-3d", Duration("3 days"), "order_total_amount", Operation.AVG),
      AggAction("total-amount-1d", Duration("1 days"), "order_total_amount", Operation.SUM),
    )

    val processor = new AggregatorProcessor(spark, endDate)

    processor.addAggAction(actions: _*).run(df)
      .write.format("delta")
      .mode("append")
      .partitionBy("year", "month", "day")
      .save(Settings.outputTable)
  }
}
