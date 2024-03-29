package br.com.ifood.data.featurestore

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers._

import java.io.File
import java.nio.file.{Files, Paths}
import scala.io.Source

class CustomFlatSpec extends AnyFlatSpecLike with BeforeAndAfterAll {

  private lazy val tmpDir = getTempDir("test")

  override def afterAll(): Unit = {
    tmpDir.delete()
  }

  def assertDataFrame(inputDf: DataFrame, expectedDf: DataFrame): Unit = {
    val input = inputDf.select(inputDf.columns.min, inputDf.columns.sorted.tail: _*).drop("fs_ingestion_timestamp")
    val expected = expectedDf.select(expectedDf.columns.min, expectedDf.columns.sorted.tail: _*).drop("fs_ingestion_timestamp")

    input.columns.deep shouldBe expected.columns.deep

    input.count() shouldBe expected.count()
    input.except(expected).count() shouldBe 0
    expected.except(input).count() shouldBe 0
    input.collect should contain theSameElementsAs expected.collect

  }

  def retrieveDataFrameFromJson(resource: String, spark: SparkSession): DataFrame = {
    val json = getClass.getResource(s"/$suiteName/datasets/$resource").toString
    spark.read.json(json)
  }

  def getResource(typeResource: String, resource: String): List[String] = {
    val fileStream = getClass.getResourceAsStream(s"/$suiteName/$typeResource/$resource")
    Source.fromInputStream(fileStream).getLines.toList
  }

  private def getTempDir(prefix: String = this.suiteName, clear: Boolean = true): File = {
    val basePath = Files.createDirectories(Paths.get("target/feature-store-tests"))
    val tmpDir = Files.createTempDirectory(basePath.toAbsolutePath, s"${prefix}_").toFile
    tmpDir
  }
}
