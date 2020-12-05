package br.com.ifood.data.featurestore.ingestion.runner

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.execution.streaming.{LongOffset, MemoryStream}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers._

class AggregatorProcessorTest extends AnyFlatSpecLike with DataFrameSuiteBase  {

  it should "test stream" in {
//    import spark.implicits._
//    val events = MemoryStream[String]
//
//    val sessions = events.toDS
//    sessions.isStreaming shouldBe true
//    val runner = new Runner(spark)
//
//
//    val schemaUntyped = new StructType()
//      .add("a", "int")
//      .add("b", "string")
//
//    val transformedSessions = sessions.select(from_json($"value", schemaUntyped).as("rsvp"))
//    val streamingQuery = transformedSessions
//      .writeStream
//      .format("memory")
//      .queryName("rawRsvp")
//      .outputMode("append")
//      .start
//    val sampleRsvp =
//      """
//      {"a": 1, "b":"oi"}
//      {"a": 1, "b":"oi"}
//      {"a": 1, "b":"oi"}
//      """.stripMargin
//
//    val batch = sampleRsvp
//
//    val currentOffset = events.addData(batch)
//    streamingQuery.processAllAvailable()
//    events.commit(currentOffset.asInstanceOf[LongOffset])
//    val a = spark.sql("select * from rawRsvp")
//    a.show()
  }

}
