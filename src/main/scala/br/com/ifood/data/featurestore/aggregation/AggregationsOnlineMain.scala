package br.com.ifood.data.featurestore.aggregation

import br.com.ifood.data.featurestore.aggregation.config.Settings
import br.com.ifood.data.featurestore.aggregation.pipeline.ParserOrder
import io.delta.tables._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.time.LocalDateTime

@Deprecated
object AggregationsOnlineMain {
  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  def createAllTablesIfNotExist(spark: SparkSession): Unit = {
    if (!DeltaTable.isDeltaTable(Settings.outputTable)) {
      import spark.implicits._
      Seq(
        ("null", 0, 0, 0, 0, Map("0" -> 0.0))
      ).toDF("key", "fs_year", "fs_month", "fs_day", "fs_hour", "features")
        .write.format("delta").save(Settings.outputTable)
    }
  }

  def main(args: Array[String]): Unit = {
    //    Settings.load(args)
    Settings.load(Array("dev",
      "dev",
      "-yarn-mode", "local[*]",
      "-input-data-table", "/tmp/ifood/data/ingestion/order-events/",
      "-output-data-table", "/tmp/ifood/data/aggregations/full-state/order-agg",
      "-temp-dir", "tempDir",
      "-watermark", "1 seconds",
      "-time-field", "fs_ingestion_timestamp",
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

    createAllTablesIfNotExist(spark)

    def upsertToDelta(microBatchOutputDF: DataFrame, batchId: Long) {
      lazy val deltaTable = DeltaTable.forPath(Settings.outputTable)
      deltaTable.as("t")
        .merge(
          microBatchOutputDF.toDF.as("s"),
          "s.key = t.key AND s.fs_year = t.fs_year AND s.fs_month = t.fs_month AND s.fs_day = t.fs_day")
        .whenMatched().updateAll()
        .whenNotMatched().insertAll()
        .execute()
    }

    val df = spark.readStream
      .format("delta")
      .option("ignoreDeletes", "true")
      .option("maxFilesPerTrigger", 1000)
      .load(s"${Settings.inputTable}")

    val processor = new ParserOrder(df, spark)

    processor.parse
      .writeStream
      .format("delta")
      .foreachBatch(upsertToDelta _)
      .outputMode("update")
      .option("checkpointLocation", s"/tmp/ifood/metadata/aggregations/_checkpoints/full-state-agg")
      .start(Settings.outputTable)
      .awaitTermination()
  }
}
