package br.com.ifood.data.featurestore.ingestion.runner

import br.com.ifood.data.featurestore.ingestion.CustomFlatSpec
import br.com.ifood.data.featurestore.ingestion.config.Settings
import br.com.ifood.data.featurestore.ingestion.model.Event
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.execution.streaming.{LongOffset, MemoryStream}
import org.apache.spark.sql.functions.col
import org.scalatest.matchers.should.Matchers._

import scala.io.Source

class RunnerOrderTest extends CustomFlatSpec with DataFrameSuiteBase {
  def getSampleStreamData(): List[Event] = {
    getResource("datasets", "input.json")
      .map(Event("1", _))
  }


  it should "test stream" in {
    import spark.implicits._
    Settings.load(Array("dev"))
    val events = MemoryStream[Event]
    val sessions = events.toDS
    sessions.isStreaming shouldBe true
    val currentOffset = events.addData(getSampleStreamData())
    val runner = new Runner(sessions, spark)

    runner.start()
      .writeStream
      .format("memory")
      .queryName("rawRsvp")
      .outputMode("append")
      .start
      .processAllAvailable()
    events.commit(currentOffset.asInstanceOf[LongOffset])

    val a = spark.sql("select * from rawRsvp")
    a.select(col("items.garnishItems")).show()
    a.printSchema()
    a.show(false)

  }

}
