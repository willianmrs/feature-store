package br.com.ifood.data.featurestore.aggregation

import java.time.LocalDateTime

import br.com.ifood.data.featurestore.aggregation.config.Settings
import br.com.ifood.data.featurestore.aggregation.pipeline.ParserOrder
import io.delta.tables._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object AggregationsOnlineMain {
  private lazy val logger = LoggerFactory.getLogger(this.getClass)


  def main(args: Array[String]): Unit = {
    //    Settings.load(args)
    Settings.load(Array("dev",
      "dev",
      "-yarn-mode", "local[*]",
      "-output-data-table", "/tmp/ifood/data/agg3/",
      "-input-data-table", "/tmp/ifood/data/ingestion/order-events",
      "-temp-dir", "tempDir",
      //      "-trigger-process-type", "30 seconds",
      //      "-stream-type", "order"
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

    def upsertToDelta(microBatchOutputDF: DataFrame, batchId: Long) {
      lazy val deltaTable = DeltaTable.forPath(Settings.outputTable)
      deltaTable.as("t")
        .merge(
          microBatchOutputDF.toDF.as("s"),
          "s.key = t.key AND s.year = t.year AND s.month = t.month AND s.day = t.day")
        .whenMatched().updateAll()
        .whenNotMatched().insertAll()
        .execute()
    }

    val df = spark.readStream
      .format("delta")
      .option("ignoreDeletes", "true")
      .load(s"${Settings.inputTable}")

    val processor = new ParserOrder(df, spark)

      processor.parse
      .writeStream
      .format("delta")
//      .foreachBatch(upsertToDelta _)
      .outputMode("append")
      .option("checkpointLocation", s"/tmp/ifood/agg/_checkpoints/first_agg3")
      .start(Settings.outputTable)
        .awaitTermination()
  }
}
