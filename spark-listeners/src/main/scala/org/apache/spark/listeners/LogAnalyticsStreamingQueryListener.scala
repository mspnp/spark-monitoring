package org.apache.spark.listeners

import java.time.Instant

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.{LogAnalytics, LogAnalyticsListenerConfiguration, SparkConf}

class LogAnalyticsStreamingQueryListener(sparkConf: SparkConf) extends StreamingQueryListener
  with Logging with LogAnalytics {

  val config = new LogAnalyticsListenerConfiguration(sparkConf)

  // TODO event.id.timestamp() check id is UUID??
  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = logSparkListenerEvent(event)


  // query progress event timestamp is string version of Instant
  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = logSparkListenerEvent(
    event,
    () => Instant.parse(event.progress.timestamp)
  )

  // TODO event.id.timestamp() check id is UUID??
  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = logSparkListenerEvent(event)
}
