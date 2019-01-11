package org.apache.spark

import java.time.Instant

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.internal.Logging
import org.apache.spark.listeners.microsoft.pnp.loganalytics.LogAnalyticsClient
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.streaming.scheduler.StreamingListenerEvent
import org.apache.spark.util.JsonProtocol
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import scala.util.control.NonFatal

case class TimeGenerated(TimeGenerated: String)

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


  protected def logStreamingListenerEvent[T <: StreamingListenerEvent](event: T,
                                                                       getTimestamp: () => Instant =
                                                                       () => Instant.now()): Unit = {
    val json = try {
      Some(
        parse(mapper.writeValueAsString(event))
          .merge(render(
            "TimeGenerated" -> getTimestamp().toString
          ))
      )
    } catch {
      case NonFatal(e) =>
        logError(s"Error serializing StreamingListenerEvent to JSON", e)
        logError(s"event: $event")
        None
    }

    logEvent(json)
  }

  protected def logSparkListenerEvent[T <: SparkListenerEvent](
                                                                event: T,
                                                                getTimestamp: () => Instant =
                                                                () => Instant.now()): Unit = {
    val json = try {
      Some(
        JsonProtocol.sparkEventToJson(event)
          .merge(render(
            "TimeGenerated" -> getTimestamp().toString
          ))
      )
    } catch {
      case NonFatal(e) =>
        logError(s"Error serializing SparkListenerEvent to JSON", e)
        logError(s"event: $event")
        None
    }

    logEvent(json)
  }

  private[spark] def logEvent(json: Option[JValue]): Unit = {
    try {
      json match {
        case Some(j) => {
          val jsonString = compact(j)
          logDebug(s"Sending event to Log Analytics: ${jsonString}")
          logAnalyticsClient.send(jsonString, config.logType, "TimeGenerated")
        }
        case None => {
          logWarning("json value was None")
        }
      }
    } catch {
      case NonFatal(e) =>
        logError(s"Error sending to Log Analytics: $e")
    }
  }
}
