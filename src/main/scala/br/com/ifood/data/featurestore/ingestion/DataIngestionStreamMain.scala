package br.com.ifood.data.featurestore.ingestion

import java.time.LocalDateTime

import br.com.ifood.data.featurestore.ingestion.config.Settings
import br.com.ifood.data.featurestore.ingestion.model.Event
import br.com.ifood.data.featurestore.ingestion.runner.Runner
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory


object DataIngestionStreamMain {

  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    //    Settings.load(args)
    Settings.load(Array("dev",
      "dev",
      "-kafka-topics", "kafkaTopics",
      "-yarn-mode", "local[*]",
      "-kafka-brokers", "kafkaBrokers",
      "-data-dir", "/tmp/ifood/ingestion/data/",
      "-temp-dir", "tempDir",
      "-stream-type", "order"
    ) )

    logger.info(s"JobName: ${Settings} started at: ${LocalDateTime.now}")

    val spark = SparkSession
      .builder()
      .appName(Settings.appName)
      .master(Settings.yarnMode)
      .config(new SparkConf()
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      )
      .getOrCreate()

    import spark.implicits._

    val df = spark.readStream
      .format("text")
      .option("maxFilesPerTrigger", 1)
      .load("/tmp/ifood/input/")
      .withColumn("key", lit("1"))
      .as[Event]


    val result = Runner(df, spark).start()

    result.writeStream
//      .outputMode("append")
//      .format("console")
//      .start()
      .format("delta")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .outputMode("append")
      .option("checkpointLocation", s"/tmp/ifood/ingestion/_checkpoints/raw_${Settings.streamType}")
      .start(s"${Settings.outputDirectory}/raw_${Settings.streamType}")
      .awaitTermination()

  }
}
