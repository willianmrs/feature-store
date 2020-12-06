package br.com.ifood.data.featurestore.ingestion.runner

import br.com.ifood.data.featurestore.CustomFlatSpec
import br.com.ifood.data.featurestore.ingestion.config.Settings
import br.com.ifood.data.featurestore.ingestion.model.Event
import br.com.ifood.data.featurestore.ingestion.parser.{ParserFactory, ParserOrder}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.execution.streaming.{LongOffset, MemoryStream}
import org.scalatest.matchers.should.Matchers._

class ParserOrderTest extends CustomFlatSpec with DataFrameSuiteBase {
  import spark.implicits._

  def getSampleStreamData(): List[Event] = {
    getResource("datasets", "input.json")
    .map(Event("1", _))
  }

  it should "process order input correctly" in {
    Settings.load(Array("dev", "-stream-type", "order-events"))
    val events = MemoryStream[Event]
    val eventsStream = events.toDS
    eventsStream.isStreaming shouldBe true
    val currentOffset = events.addData(getSampleStreamData())
    val parser = ParserFactory(Settings.streamType, spark)

    parser.parse(events.toDF())
      .writeStream
      .format("memory")
      .queryName("outputTable")
      .outputMode("append")
      .start
      .processAllAvailable()
    events.commit(currentOffset.asInstanceOf[LongOffset])

    val dfOutput = spark.sql("select * from outputTable")
    val dfExpected = retrieveDataFrameFromJson("output", spark)

    // TODO: test nested columns.
//    assertDataFrame(dfOutput.drop("items"), dfExpected.drop("items"))
  }


  it should "parse date correctly" in {
    val df = Seq(
      "2019-01-17T22:50:06.000Z",
      "2019-01-17T22:50:06.000Z",
      "2019-01-17T22:50:06.000Z",
    ).toDF("date")

    df.show()
    df.printSchema()

    val parser = new ParserOrder(spark)
    val result = parser.parseDate("date")(df)

    result.show
  }



}
