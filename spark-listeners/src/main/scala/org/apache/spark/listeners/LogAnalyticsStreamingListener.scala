package org.apache.spark.listeners

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.scheduler._
import org.apache.spark.{LogAnalytics, LogAnalyticsListenerConfiguration, SparkConf}

class LogAnalyticsStreamingListener(sparkConf: SparkConf) extends StreamingListener
  with Logging with LogAnalytics {

  val config = new LogAnalyticsListenerConfiguration(sparkConf)

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    logStreamingListenerEvent(streamingStarted)
  }

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
    logStreamingListenerEvent(receiverStarted)
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {
    logStreamingListenerEvent(receiverError)
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = {
    logStreamingListenerEvent(receiverStopped)
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    logStreamingListenerEvent(batchSubmitted)
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
    logStreamingListenerEvent(batchStarted)
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    logStreamingListenerEvent(batchCompleted)
  }

  override def onOutputOperationStarted(outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = {
    logStreamingListenerEvent(outputOperationStarted)
  }

  override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = {
    logStreamingListenerEvent(outputOperationCompleted)
  }
}
