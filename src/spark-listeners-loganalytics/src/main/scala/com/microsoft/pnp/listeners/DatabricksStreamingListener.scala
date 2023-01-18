package com.microsoft.pnp.listeners

import org.apache.logging.log4j.LogManager
import org.apache.spark.streaming.scheduler._

class DatabricksStreamingListener extends StreamingListener {

  private val LOGGER = LogManager.getLogger();

  private def processStreamingEvent(listenerEvent: StreamingListenerEvent): Unit = {
    try {
      ListenerUtils.sendStreamingEventToLA(listenerEvent)
    } catch {
      case e: Exception =>
        LOGGER.error("Could not parse event " + listenerEvent.getClass.getName)
        LOGGER.error(e.getMessage)
    }
  }

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = processStreamingEvent(streamingStarted)

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = processStreamingEvent(receiverStarted)

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = processStreamingEvent(receiverError)

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = processStreamingEvent(receiverStopped)

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = processStreamingEvent(batchSubmitted)

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = processStreamingEvent(batchStarted)

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = processStreamingEvent(batchCompleted)

  override def onOutputOperationStarted(outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = processStreamingEvent(outputOperationStarted)

  override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = processStreamingEvent(outputOperationCompleted)


}
