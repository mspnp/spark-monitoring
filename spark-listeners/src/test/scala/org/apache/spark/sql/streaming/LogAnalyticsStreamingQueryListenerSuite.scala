package org.apache.spark.sql.streaming

import java.util.UUID

import org.apache.spark.listeners.ListenerSuite
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.scalatest.BeforeAndAfterEach

import scala.collection.JavaConversions.mapAsJavaMap

object LogAnalyticsStreamingQueryListenerSuite {
  val queryStartedEvent = new QueryStartedEvent(UUID.randomUUID, UUID.randomUUID, "name")
  val queryTerminatedEvent = new QueryTerminatedEvent(UUID.randomUUID, UUID.randomUUID, None)
  val queryProgressEvent = new QueryProgressEvent(
    new StreamingQueryProgress(
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
      new SinkProgress("sink")
    )
  )
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
