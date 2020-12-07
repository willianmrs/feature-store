package br.com.ifood.data.featurestore.publisher

import br.com.ifood.data.featurestore.publisher.config.Settings
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.time.LocalDateTime

object PublisherMain {
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

    val publisher = PublisherFactory.apply(Settings.publisherType, spark).process(df)

    publisher.save()
  }
}