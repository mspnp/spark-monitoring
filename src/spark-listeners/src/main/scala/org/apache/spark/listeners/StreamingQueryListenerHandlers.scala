package org.apache.spark.listeners

import java.time.Instant

import org.apache.spark.sql.streaming.StreamingQueryListener

trait StreamingQueryListenerHandlers{
  this: UnifiedSparkListenerHandler =>

  private[listeners] def onStreamingQueryListenerEvent(event: StreamingQueryListener.Event): Unit = {
    // Only the query progress event has a timestamp, so we'll send everything else
    // on through
    event match {
      case queryProgress: StreamingQueryListener.QueryProgressEvent =>
        logSparkListenerEvent(
          event,
          () => Instant.parse(queryProgress.progress.timestamp)
        )
      case streamingQueryListenerEvent: StreamingQueryListener.Event =>
        logSparkListenerEvent(streamingQueryListenerEvent)
    }
  }
}
