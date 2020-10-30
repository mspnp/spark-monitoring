package org.apache.spark.sql.streaming

import java.util.UUID

import org.apache.spark.listeners.ListenerSuite
import org.apache.spark.metrics.TestImplicits._
import org.apache.spark.metrics.TestUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.scalatest.BeforeAndAfterEach

import scala.collection.JavaConversions.mapAsJavaMap

object LogAnalyticsStreamingQueryListenerSuite {
  //Spark3 requires 1 more argument than spark 2.4
  val queryStartedEvent = TestUtils
    .newInstance(classOf[QueryStartedEvent], UUID.randomUUID, UUID.randomUUID, "name")
    .orElse(TestUtils.newInstance(classOf[QueryStartedEvent], UUID.randomUUID, UUID.randomUUID, "name", (System.currentTimeMillis() / 1000).toString))
    .get

  val queryTerminatedEvent = new QueryTerminatedEvent(UUID.randomUUID, UUID.randomUUID, None)
  val queryProgressEvent = {

    val spark2args = List[Any](
      UUID.randomUUID,
      UUID.randomUUID,
      null,
      ListenerSuite.EPOCH_TIME_AS_ISO8601,
      2L,
      mapAsJavaMap(Map("total" -> 0L)),
      mapAsJavaMap(Map.empty[String, String]),
      Array(new StateOperatorProgress(
        0, 1, 2)),
      Array(
        new SourceProgress(
          "source",
          "123",
          "456",
          678,
          Double.NaN,
          Double.NegativeInfinity
        )
      ),
      new SinkProgress("sink"), mapAsJavaMap(Map[String, Row]())
    )

    val spark3args = spark2args.insertAt(4, 1234L)

    val streamingQueryProgress = TestUtils.newInstance(classOf[StreamingQueryProgress], spark2args:_*)
      .orElse(TestUtils.newInstance(classOf[StreamingQueryProgress], spark3args:_*))
      .get

    new QueryProgressEvent(streamingQueryProgress)
  }
}

class LogAnalyticsStreamingQueryListenerSuite extends ListenerSuite
  with BeforeAndAfterEach {

  test("should invoke sendToSink for QueryStartedEvent with full class name") {
    val (json, event) = this.onStreamingQueryListenerEvent(
      LogAnalyticsStreamingQueryListenerSuite.queryStartedEvent
    )

    this.assertEvent(json, event)
  }

  test("should invoke sendToSink for QueryTerminatedEvent with full class name") {
    val (json, event) = this.onStreamingQueryListenerEvent(
      LogAnalyticsStreamingQueryListenerSuite.queryTerminatedEvent
    )

    this.assertEvent(json, event)
  }

  test("should invoke sendToSink for QueryProgressEvent with full class name") {
    val (json, event) = this.onStreamingQueryListenerEvent(
      LogAnalyticsStreamingQueryListenerSuite.queryProgressEvent
    )

    this.assertEvent(json, event)
  }

  test("QueryProgressEvent should have expected SparkEventTime") {
    val (json, _) = this.onStreamingQueryListenerEvent(
      LogAnalyticsStreamingQueryListenerSuite.queryProgressEvent
    )

    this.assertSparkEventTime(
      json,
      (_, value) => assert(value.extract[String] === ListenerSuite.EPOCH_TIME_AS_ISO8601)
    )
  }

  test("QueryStartedEvent should have SparkEventTime") {
    val (json, _) = this.onStreamingQueryListenerEvent(
      LogAnalyticsStreamingQueryListenerSuite.queryStartedEvent
    )
    this.assertSparkEventTime(
      json,
      (_, value) => assert(!value.extract[String].isEmpty)
    )
  }

  test("QueryTerminatedEvent should have SparkEventTime") {
    val (json, _) = this.onStreamingQueryListenerEvent(
      LogAnalyticsStreamingQueryListenerSuite.queryTerminatedEvent
    )
    this.assertSparkEventTime(
      json,
      (_, value) => assert(!value.extract[String].isEmpty)
    )
  }
}
