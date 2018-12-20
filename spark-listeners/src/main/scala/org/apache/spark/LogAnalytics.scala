package org.apache.spark

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.internal.Logging
import org.apache.spark.listeners.microsoft.pnp.loganalytics.LogAnalyticsClient
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.streaming.scheduler.StreamingListenerEvent
import org.apache.spark.util.JsonProtocol
import org.json4s.jackson.JsonMethods.{compact, parse}

import scala.util.control.NonFatal

trait LogAnalytics {
  this: Logging =>
  protected val config: LogAnalyticsConfiguration

  protected lazy val logAnalyticsClient = new LogAnalyticsClient(
    config.workspaceId, config.secret)

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "Event")
  private class StreamingListenerEventMixIn {}

  // Add the Event field since the StreamingListenerEvents don't extend the SparkListenerEvent trait
  mapper.addMixIn(classOf[StreamingListenerEvent], classOf[StreamingListenerEventMixIn])

  protected def logEvent(event: AnyRef) {
    val s = try {
      logDebug(s"Serializing event to JSON")
      val json = event match {
        case e: SparkListenerEvent => Some(JsonProtocol.sparkEventToJson(e))
        case e: StreamingListenerEvent => Some(parse(mapper.writeValueAsString(e)))
        case e =>
          logWarning(s"Class '${e.getClass.getCanonicalName}' is not a supported event type")
          None
      }
      if (json.isDefined) {
        Some(compact(json.get))
      } else {
        None
      }
    } catch {
      case NonFatal(e) =>
        logError(s"Error sending to Log Analytics: $e")
        logError(s"event: $event")
        None
    }

    if (s.isDefined) {
      logDebug(s"Sending event to Log Analytics")
      logDebug(s.get)
      logAnalyticsClient.send(s.get, config.logType, config.timestampFieldName)
    }
  }
}
