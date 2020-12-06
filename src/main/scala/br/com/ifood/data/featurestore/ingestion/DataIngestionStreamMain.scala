package br.com.ifood.data.featurestore.ingestion

import br.com.ifood.data.featurestore.ingestion.config.Settings
import br.com.ifood.data.featurestore.ingestion.parser.ParserFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.time.LocalDateTime


object DataIngestionStreamMain {

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
    import spark.implicits._

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", Settings.kafkaBrokers)
      .option("subscribe", Settings.kafkaTopics)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", 100)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .toDF

    val parser = ParserFactory(Settings.streamType, spark)
    val pipeline = parser.parse(df)


    pipeline.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", s"/tmp/ifood/metadata/ingestion/_checkpoints/${Settings.streamType}")
      .partitionBy("fs_year", "fs_month", "fs_day")
      .start(s"${Settings.outputDirectory}/ingestion/${Settings.streamType}")
      .awaitTermination()
  }
}
