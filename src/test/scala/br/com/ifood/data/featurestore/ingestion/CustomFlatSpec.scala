package br.com.ifood.data.featurestore.ingestion

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers._

import scala.io.Source

class CustomFlatSpec extends AnyFlatSpecLike with BeforeAndAfterAll {

  private lazy val tmpDir = getTempDir("test")

  override def afterAll(): Unit = {
    tmpDir.delete()
  }

  def setEnv(key: String, value: String): String = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    val map = field.get(System.getenv()).asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
    map.put(key, value)
  }

  def assertDataFrame(inputDf: DataFrame, expectedDf: DataFrame): Unit = {
    val input = inputDf.select(inputDf.columns.min, inputDf.columns.sorted.tail: _*)
    val expected = expectedDf.select(expectedDf.columns.min, expectedDf.columns.sorted.tail: _*)

    input.columns.deep shouldBe expected.columns.deep
    input.collect should contain theSameElementsAs expected.collect
  }

  def retrieveDataFrame(resource: String, spark: SparkSession): DataFrame = {
    val json = getClass.getResource(s"/$suiteName/datasets/$resource").toString
    spark.read.option("multiline", value = true).json(json)
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
