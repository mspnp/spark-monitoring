package org.apache.spark.listeners

import org.apache.spark.streaming.Time
import org.apache.spark.streaming.scheduler._

object LogAnalyticsStreamingListenerSuite {
  val streamingListenerStreamingStarted = StreamingListenerStreamingStarted(
    ListenerSuite.EPOCH_TIME
  )
  val streamingListenerReceiverStarted = StreamingListenerReceiverStarted(
    ReceiverInfo(0, "test", true, "localhost", "0")
  )
  val streamingListenerReceiverError = StreamingListenerReceiverError(
    ReceiverInfo(1, "test", true, "localhost", "1")
  )
  val streamingListenerReceiverStopped = StreamingListenerReceiverStopped(
    ReceiverInfo(2, "test", true, "localhost", "2")
  )

  private val streamIdToInputInfo = Map(
    0 -> StreamInputInfo(0, 300L),
    1 -> StreamInputInfo(1, 300L, Map(StreamInputInfo.METADATA_KEY_DESCRIPTION -> "test")))

  val streamingListenerBatchSubmitted = StreamingListenerBatchSubmitted(
    BatchInfo(Time(1000), streamIdToInputInfo, ListenerSuite.EPOCH_TIME, None, None, Map.empty)
  )

  val streamingListenerBatchStarted = StreamingListenerBatchStarted(
    BatchInfo(Time(1000), streamIdToInputInfo, 1000, Some(ListenerSuite.EPOCH_TIME), None, Map.empty)
  )

  val streamingListenerBatchStartedNoneProcessingStartTime = StreamingListenerBatchStarted(
    BatchInfo(Time(1000), streamIdToInputInfo, 1000, None, None, Map.empty)
  )

  val streamingListenerBatchCompleted = StreamingListenerBatchCompleted(
    BatchInfo(Time(1000L), streamIdToInputInfo, 1000, Some(2000), Some(ListenerSuite.EPOCH_TIME), Map.empty)
  )

  val streamingListenerBatchCompletedNoneProcessingEndTime = StreamingListenerBatchCompleted(
    BatchInfo(Time(ListenerSuite.EPOCH_TIME), streamIdToInputInfo, 1000, Some(2000), None, Map.empty)
  )

  val streamingListenerOutputOperationStarted = StreamingListenerOutputOperationStarted(
    OutputOperationInfo(
      Time(1000L),
      0,
      "op1",
      "operation1",
      Some(ListenerSuite.EPOCH_TIME),
      None,
      None
    )
  )

  val streamingListenerOutputOperationStartedNoneStartTime = StreamingListenerOutputOperationStarted(
    OutputOperationInfo(
      Time(1000L),
      0,
      "op1",
      "operation1",
      None,
      None,
      None
    )
  )

  val streamingListenerOutputOperationCompleted = StreamingListenerOutputOperationCompleted(
    OutputOperationInfo(
      Time(1000L),
      0,
      "op1",
      "operation1",
      Some(1003L),
      Some(ListenerSuite.EPOCH_TIME),
      None
  ))

  val streamingListenerOutputOperationCompletedNoneEndTime = StreamingListenerOutputOperationCompleted(
    OutputOperationInfo(
      Time(1000L),
      0,
      "op1",
      "operation1",
      Some(1003L),
      None,
      None
    ))
}

class LogAnalyticsStreamingListenerSuite extends ListenerSuite {
  test("should invoke sendToSink for StreamingListenerStreamingStarted event with full class name") {
    val (json, event) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerStreamingStarted
    )

    this.assertEvent(json, event)
  }

  test("should invoke sendToSink for StreamingListenerReceiverStarted event with full class name") {
    val (json, event) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerReceiverStarted
    )

    this.assertEvent(json, event)
  }

  test("should invoke sendToSink for StreamingListenerReceiverError event with full class name") {
    val (json, event) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerReceiverError
    )

    this.assertEvent(json, event)
  }

  test("should invoke sendToSink for StreamingListenerReceiverStopped event with full class name") {
    val (json, event) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerReceiverStopped
    )

    this.assertEvent(json, event)
  }

  test("should invoke sendToSink for StreamingListenerBatchSubmitted event with full class name") {
    val (json, event) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerBatchSubmitted
    )

    this.assertEvent(json, event)
  }

  test("should invoke sendToSink for StreamingListenerBatchStarted event with full class name") {
    val (json, event) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerBatchStarted
    )

    this.assertEvent(json, event)
  }

  test("should invoke sendToSink for StreamingListenerBatchCompleted event with full class name") {
    val (json, event) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerBatchCompleted
    )

    this.assertEvent(json, event)
  }

  test("should invoke sendToSink for StreamingListenerOutputOperationStarted event with full class name") {
    val (json, event) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerOutputOperationStarted
    )

    this.assertEvent(json, event)
  }

  test("should invoke sendToSink for StreamingListenerOutputOperationCompleted event with full class name") {
    val (json, event) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerOutputOperationCompleted
    )

    this.assertEvent(json, event)
  }

  test("StreamingListenerStreamingStarted should have expected SparkEventTime") {
    val (json, _) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerStreamingStarted
    )
    this.assertSparkEventTime(
      json,
      (_, value) => assert(value.extract[String] === ListenerSuite.EPOCH_TIME_AS_ISO8601)
    )
  }

  test("StreamingListenerBatchCompleted should have expected SparkEventTime") {
    val (json, _) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerBatchCompleted
    )
    this.assertSparkEventTime(
      json,
      (_, value) => assert(value.extract[String] === ListenerSuite.EPOCH_TIME_AS_ISO8601)
    )
  }

  test("StreamingListenerBatchSubmitted should have expected SparkEventTime") {
    val (json, _) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerBatchSubmitted
    )
    this.assertSparkEventTime(
      json,
      (_, value) => assert(value.extract[String] === ListenerSuite.EPOCH_TIME_AS_ISO8601)
    )
  }

  test("StreamingListenerBatchStarted should have expected SparkEventTime") {
    val (json, _) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerBatchStarted
    )
    this.assertSparkEventTime(
      json,
      (_, value) => assert(value.extract[String] === ListenerSuite.EPOCH_TIME_AS_ISO8601)
    )
  }

  test("StreamingListenerBatchStarted with no processingStartTime should have SparkEventTime") {
    val (json, _) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerBatchStartedNoneProcessingStartTime
    )
    this.assertSparkEventTime(
      json,
      (_, value) => assert(!value.extract[String].isEmpty)
    )
  }

  test("StreamingListenerOutputOperationCompleted should have expected SparkEventTime") {
    val (json, _) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerOutputOperationCompleted
    )
    this.assertSparkEventTime(
      json,
      (_, value) => assert(value.extract[String] === ListenerSuite.EPOCH_TIME_AS_ISO8601)
    )
  }

  test("StreamingListenerOutputOperationCompleted with no endTime should have SparkEventTime") {
    val (json, _) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerOutputOperationCompleted
    )
    this.assertSparkEventTime(
      json,
      (_, value) => assert(!value.extract[String].isEmpty)
    )
  }

  test("StreamingListenerOutputOperationStarted should have expected SparkEventTime") {
    val (json, _) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerOutputOperationStarted
    )
    this.assertSparkEventTime(
      json,
      (_, value) => assert(value.extract[String] === ListenerSuite.EPOCH_TIME_AS_ISO8601)
    )
  }

  test("StreamingListenerOutputOperationStarted with no endTime should have SparkEventTime") {
    val (json, _) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerOutputOperationStarted
    )
    this.assertSparkEventTime(
      json,
      (_, value) => assert(!value.extract[String].isEmpty)
    )
  }

  test("StreamingListenerReceiverError should have SparkEventTime") {
    val (json, _) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerReceiverError
    )
    this.assertSparkEventTime(
      json,
      (_, value) => assert(!value.extract[String].isEmpty)
    )
  }

  test("StreamingListenerReceiverStarted should have SparkEventTime") {
    val (json, _) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerReceiverStarted
    )
    this.assertSparkEventTime(
      json,
      (_, value) => assert(!value.extract[String].isEmpty)
    )
  }

  test("StreamingListenerReceiverStopped should have SparkEventTime") {
    val (json, _) = this.onStreamingListenerEvent(
      LogAnalyticsStreamingListenerSuite.streamingListenerReceiverStopped
    )
    this.assertSparkEventTime(
      json,
      (_, value) => assert(!value.extract[String].isEmpty)
    )
  }
}
