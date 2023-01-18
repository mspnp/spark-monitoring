package com.microsoft.pnp.listeners

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.streaming.StreamingQueryListener

class DatabricksStreamingQueryListener extends StreamingQueryListener{

  private val LOGGER = LogManager.getLogger();

  private def processStreamingEvent(event: StreamingQueryListener.Event): Unit = {
    try {
      ListenerUtils.sendStreamingQueryEventToLA(event)
    } catch {
      case e: Exception =>
        LOGGER.error("Could not parse event " + event.getClass.getName)
        LOGGER.error(e.getMessage)
    }
  }
  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = processStreamingEvent(event)

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = processStreamingEvent(event)

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = processStreamingEvent(event)
}
