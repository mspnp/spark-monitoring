package org.apache.spark.listeners

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.{LogAnalytics, LogAnalyticsListenerConfiguration, SparkConf}

class LogAnalyticsStreamingQueryListener(sparkConf: SparkConf) extends StreamingQueryListener
  with Logging with LogAnalytics {

  val config = new LogAnalyticsListenerConfiguration(sparkConf)

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    logSparkListenerEvent(event)
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    logSparkListenerEvent(event)
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    logSparkListenerEvent(event)
  }
}
