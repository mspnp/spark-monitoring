package com.microsoft.pnp.listeners

import org.apache.logging.log4j.LogManager
import org.apache.spark.SparkFirehoseListener
import org.apache.spark.scheduler.SparkListenerEvent

class UltimateListener extends SparkFirehoseListener {

  private val LOGGER = LogManager.getLogger();

  override def onEvent(listenerEvent: SparkListenerEvent): Unit = {
    try {
      ListenerUtils.sendListenerEventToLA(listenerEvent)
    } catch {
      case e: Exception =>
        LOGGER.error("Could not parse event " + listenerEvent.getClass.getName)
        LOGGER.error(e.getMessage)
    }
  }
}
