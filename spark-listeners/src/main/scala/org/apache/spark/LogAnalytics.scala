package org.apache.spark

import java.time.Instant

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver
import com.fasterxml.jackson.databind.{DatabindContext, JavaType, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.internal.Logging
import org.apache.spark.listeners.microsoft.pnp.loganalytics.{LogAnalyticsBufferedClient, LogAnalyticsClient}
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.streaming.scheduler.StreamingListenerEvent
import org.apache.spark.util.JsonProtocol
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import scala.util.control.NonFatal

case class TimeGenerated(TimeGenerated: String)

private class StreamingListenerEventTypeIdResolver extends com.fasterxml.jackson.databind.jsontype.TypeIdResolver {
  private var javaType: JavaType = _

  override def init(javaType: JavaType): Unit = {
    this.javaType = javaType
  }

  override def idFromValue(o: Any): String = this.idFromValueAndType(o, o.getClass)

  override def idFromValueAndType(o: Any, aClass: Class[_]): String =
    org.apache.spark.util.Utils.getFormattedClassName(o.asInstanceOf[AnyRef])

  override def idFromBaseType(): String = throw new NotImplementedError()

  override def typeFromId(s: String): JavaType = throw new NotImplementedError()

  override def typeFromId(databindContext: DatabindContext, s: String): JavaType = throw new NotImplementedError()

  override def getMechanism: JsonTypeInfo.Id = JsonTypeInfo.Id.CUSTOM
}

trait LogAnalytics {
  this: Logging =>
  protected val config: LogAnalyticsConfiguration


  protected lazy val logAnalyticsBufferedClient = new LogAnalyticsBufferedClient(
    new LogAnalyticsClient(
      config.workspaceId, config.secret),
    config.logType
  )

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  @JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = JsonTypeInfo.As.PROPERTY, property = "Event")
  @JsonTypeIdResolver(classOf[StreamingListenerEventTypeIdResolver])
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
            "SparkEventTime" -> getTimestamp().toString
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
            "SparkEventTime" -> getTimestamp().toString
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
          logAnalyticsBufferedClient.sendMessage(jsonString, "SparkEventTime")
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
