package org.apache.spark.listeners

class LogAnalyticsStreamingListenerSuite extends ListenerSuite[LogAnalyticsStreamingListener] {

  test("should invoke onStreamingStarted ") {
    this.onStreamingListenerEvent(this.listener.onStreamingStarted)
  }

  test("should invoke onReceiverStarted ") {
    this.onStreamingListenerEvent(this.listener.onReceiverStarted)
  }

  test("should invoke onReceiverError ") {
    this.onStreamingListenerEvent(this.listener.onReceiverError)
  }

  test("should invoke onReceiverStopped ") {
    this.onStreamingListenerEvent(this.listener.onReceiverStopped)
  }

  test("should invoke onBatchSubmitted ") {
    this.onStreamingListenerEvent(this.listener.onBatchSubmitted)
  }

  test("should invoke onBatchStarted ") {
    this.onStreamingListenerEvent(this.listener.onBatchStarted)
  }

  test("should invoke onBatchCompleted ") {
    this.onStreamingListenerEvent(this.listener.onBatchCompleted)
  }

  test("should invoke onOutputOperationStarted ") {
    this.onStreamingListenerEvent(this.listener.onOutputOperationStarted)
  }

  test("should invoke onOutputOperationCompleted ") {
    this.onStreamingListenerEvent(this.listener.onOutputOperationCompleted)
  }

  test("onStreamingStarted with  time  should populate expected TimeGenerated") {
    val event = SparkTestEvents.streamingListenerStreamingStartedEvent
    this.assertTimeGenerated(
      this.onStreamingListenerEvent(this.listener.onStreamingStarted, event),
      t => assert(t._2.extract[String] == SparkTestEvents.EPOCH_TIME_AS_ISO8601)
    )
  }

  test("onReceiverStarted with no time field should populate TimeGeneratedField") {
    val event = SparkTestEvents.streamingListenerReceiverStartedEvent
    this.assertTimeGenerated(
      this.onStreamingListenerEvent(this.listener.onReceiverStarted, event),
      t => assert(!t._2.extract[String].isEmpty)
    )
  }
}
