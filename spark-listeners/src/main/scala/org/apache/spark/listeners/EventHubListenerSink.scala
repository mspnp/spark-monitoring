package org.apache.spark.listeners

import java.util.concurrent.Executors

import com.microsoft.azure.eventhubs.ConnectionStringBuilder
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.listeners.sink.eventhubs.EventHubsSendBufferClient
import org.json4s.JsonAST
import org.json4s.jackson.JsonMethods.compact

import scala.util.control.NonFatal

class EventHubListenerSink(conf: SparkConf) extends SparkListenerSink with Logging {
  //private val config = new LogAnalyticsListenerConfiguration(conf)


  protected lazy val client = {
    val connStr = new ConnectionStringBuilder()
      .setNamespaceName("----ServiceBusNamespaceName-----")
      .setEventHubName("----EventHubName-----")
      .setSasKeyName("-----SharedAccessSignatureKeyName-----")
      .setSasKey("---SharedAccessSignatureKey----")

    val executorService = Executors.newScheduledThreadPool(1)
//    val sender = EventHubClient.createSync(connStr.toString, executorService)
//    new EventHubsSendBufferClient(sender)
    new EventHubsSendBufferClient(null)
  }

  override def logEvent(event: Option[JsonAST.JValue]): Unit = {
    try {
      event match {
        case Some(j) => {
          val jsonString = compact(j)
          logDebug(s"Sending event to Event Hub: ${jsonString}")
          client.sendMessage(jsonString)
        }
        case None =>
      }
    } catch {
      case NonFatal(e) =>
        logError(s"Error sending to Event Hub: $e")
    }
  }
}
