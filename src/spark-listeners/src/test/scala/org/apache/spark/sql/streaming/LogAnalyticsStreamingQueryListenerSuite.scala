package org.apache.spark.sql.streaming

import java.util.UUID

import org.apache.spark.listeners.ListenerSuite
import org.apache.spark.metrics.TestImplicits._
import org.apache.spark.metrics.TestUtils._
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.scalatest.BeforeAndAfterEach

import scala.collection.JavaConversions.mapAsJavaMap

object LogAnalyticsStreamingQueryListenerSuite {
  //Spark3 requires 1 more argument than spark 2.4
  val queryStartedEvent =
    newInstance(classOf[QueryStartedEvent], UUID.randomUUID, UUID.randomUUID, "name")
    .orElse(newInstance(classOf[QueryStartedEvent], UUID.randomUUID, UUID.randomUUID, "name", (System.currentTimeMillis() / 1000).toString))
    .get

  val queryTerminatedEvent = new QueryTerminatedEvent(UUID.randomUUID, UUID.randomUUID, None)
  val queryProgressEvent = {
    // v3.0.1-: StateOperatorProgress: (numRowsTotal: Long, numRowsUpdated: Long, memoryUsedBytes: Long, customMetrics: java.util.Map[String,Long])
    // v3.1.* : StateOperatorProgress: (numRowsTotal: Long, numRowsUpdated: Long, memoryUsedBytes: Long, numRowsDroppedByWatermark: Long, customMetrics: java.util.Map[String,Long])
    // v3.2.0+: StateOperatorProgress: (operatorName: String, numRowsTotal: Long, numRowsUpdated: Long, allUpdatesTimeMs: Long, numRowsRemoved: Long, allRemovalsTimeMs: Long, commitTimeMs: Long, memoryUsedBytes: Long, numRowsDroppedByWatermark: Long, numShufflePartitions: Long, numStateStoreInstances: Long, customMetrics: java.util.Map[String,Long])
    val v30argsStateOperatorProgress = List[Any](0, 1, 2, new java.util.HashMap())
    val v31argsStateOperatorProgress = List[Any](0, 1, 2, 3, new java.util.HashMap())
    val v32argsStateOperatorProgress = List[Any]("operatorName", 0, 1, 10, 2, 10, 10, 3, 4, 5, 6, new java.util.HashMap())

    val stateOperatorProgress = newInstance(classOf[StateOperatorProgress], v30argsStateOperatorProgress:_*)
      .orElse(newInstance(classOf[StateOperatorProgress], v31argsStateOperatorProgress:_*))
      .orElse(newInstance(classOf[StateOperatorProgress], v32argsStateOperatorProgress:_*))
      .get

    // v3.1.2-: SourceProgress: (description: String, startOffset: String, endOffset: String, numInputRows: Long, inputRowsPerSecond: Double, processedRowsPerSecond: Double)
    // v3.2.0+: SourceProgress: (description: String, startOffset: String, endOffset: String, latestOffset: String, numInputRows: Long, inputRowsPerSecond: Double, processedRowsPerSecond: Double, metrics: java.util.Map[String,String])
    val v31argsSourceProgress = List[Any]("source", "123", "456", 678, Double.NaN, Double.NegativeInfinity)
    val v32argsSourceProgress = List[Any]("source", "123", "456", "789", 1000, Double.NaN, Double.NegativeInfinity, new java.util.HashMap())

    val spark2args = List[Any](
      UUID.randomUUID,
      UUID.randomUUID,
      "test",
      ListenerSuite.EPOCH_TIME_AS_ISO8601,
      2L,
      mapAsJavaMap(Map("total" -> 0L)),
      mapAsJavaMap(Map.empty[String, String]),
      Array(stateOperatorProgress),
      Array(
        newInstance(classOf[SourceProgress], v31argsSourceProgress:_*)
        .orElse(newInstance(classOf[SourceProgress], v32argsSourceProgress:_*))
        .get
      ),
      new SinkProgress("sink")
    )

    val spark3args = spark2args.insertAt(4, 1234L) ::: List(mapAsJavaMap(Map[String, Row]()))

    val streamingQueryProgress = newInstance(classOf[StreamingQueryProgress], spark2args:_*)
      .orElse(newInstance(classOf[StreamingQueryProgress], spark3args:_*))
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
