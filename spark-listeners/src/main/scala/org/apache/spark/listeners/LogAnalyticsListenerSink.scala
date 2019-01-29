package org.apache.spark.listeners

import org.apache.spark.internal.Logging
import org.apache.spark.listeners.microsoft.pnp.loganalytics.{LogAnalyticsClient, LogAnalyticsSendBufferClient}
import org.apache.spark.{LogAnalyticsListenerConfiguration, SparkConf}
import org.json4s.JsonAST
import org.json4s.jackson.JsonMethods.compact

import scala.util.control.NonFatal

class LogAnalyticsListenerSink(conf: SparkConf) extends SparkListenerSink with Logging {
  private val config = new LogAnalyticsListenerConfiguration(conf)


  //protected lazy val logAnalyticsBufferedClient = new LogAnalyticsBufferedClient(
  protected lazy val logAnalyticsBufferedClient = new LogAnalyticsSendBufferClient(
    new LogAnalyticsClient(
      config.workspaceId, config.secret),
    config.logType
  )

  override def logEvent(event: Option[JsonAST.JValue]): Unit = {
    try {
      event match {
        case Some(j) => {
          val jsonString = compact(j)
          logDebug(s"Sending event to Log Analytics: ${jsonString}")
          logAnalyticsBufferedClient.sendMessage(jsonString, "SparkEventTime")
        }
        case None =>
      }
    } catch {
      case NonFatal(e) =>
        logError(s"Error sending to Log Analytics: $e")
    }
  }
}
