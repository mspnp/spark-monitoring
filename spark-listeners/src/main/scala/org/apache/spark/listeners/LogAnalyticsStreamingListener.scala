package org.apache.spark.listeners

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.scheduler._
import org.apache.spark.{LogAnalytics, LogAnalyticsListenerConfiguration, SparkConf}

class LogAnalyticsStreamingListener(sparkConf: SparkConf) extends StreamingListener
  with Logging with LogAnalytics {

  val config = new LogAnalyticsListenerConfiguration(sparkConf)

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    logEvent(streamingStarted)
  }

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
    logEvent(receiverStarted)
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {
    logEvent(receiverError)
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = {
    logEvent(receiverStopped)
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    logEvent(batchSubmitted)
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
    logEvent(batchStarted)
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    logEvent(batchCompleted)
  }

  override def onOutputOperationStarted(outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = {
    logEvent(outputOperationStarted)
  }

  override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = {
    logEvent(outputOperationCompleted)
  }
}
