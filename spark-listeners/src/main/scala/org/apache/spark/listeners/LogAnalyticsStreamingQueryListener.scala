package org.apache.spark.listeners

import java.time.Instant

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.{LogAnalytics, LogAnalyticsListenerConfiguration, SparkConf}

class LogAnalyticsStreamingQueryListener(override val conf: SparkConf) extends StreamingQueryListener
  with Logging with LogAnalytics {

  //val config = new LogAnalyticsListenerConfiguration(conf)

  //event.id.timestamp. here is id is of type UUID and UUID is generated based on random strategy and this timestamp is
  //not helpful to know when this event happened

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = logSparkListenerEvent(event)


  // query progress event timestamp is string version of Instant
  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = logSparkListenerEvent(
    event,
    () => Instant.parse(event.progress.timestamp)
  )

  //event.id.timestamp. here is id is of type UUID and UUID is generated based on random strategy and this timestamp is
  //not helpful to know when this event happened
  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = logSparkListenerEvent(event)
}
