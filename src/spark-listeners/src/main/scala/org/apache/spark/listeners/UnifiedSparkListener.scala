package org.apache.spark.listeners

import java.time.Instant

import com.microsoft.pnp.SparkInformation
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.listeners.sink.SparkListenerSink
import org.apache.spark.scheduler._
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.util.JsonProtocol
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

import scala.util.control.NonFatal

/**
  * A unified SparkListener that logs events to a configured sink.
  *
  */
class UnifiedSparkListener(override val conf: SparkConf)
  extends UnifiedSparkListenerHandler
    with Logging
    with SparkListenerHandlers
    with StreamingListenerHandlers
    with StreamingQueryListenerHandlers {

  private val listenerSink = this.createSink(this.conf)

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    // All events in Spark that are not specific to SparkListener go through
    // this method.  The typed ListenerBus implementations intercept and forward to
    // their "local" listeners.
    // We will just handle everything here so we only have to have one listener.
    // The advantage is that this can be registered in extraListeners, so no
    // code change is required to add listener support.
    event match {
      // We will use the ClassTag for the private wrapper class to match
      case this.streamingListenerEventClassTag(e) =>
        this.onStreamingListenerEvent(e)
      case streamingQueryListenerEvent: StreamingQueryListener.Event =>
        this.onStreamingQueryListenerEvent(streamingQueryListenerEvent)
      case sparkListenerEvent: SparkListenerEvent => if (sparkListenerEvent.logEvent) {
        logSparkListenerEvent(sparkListenerEvent)
      }
    }
  }

  private def createSink(conf: SparkConf): SparkListenerSink = {
    val sink = conf.getOption("spark.unifiedListener.sink") match {
      case Some(listenerSinkClassName) => listenerSinkClassName
      case None => throw new SparkException("spark.unifiedListener.sink setting is required")
    }
    logInfo(s"Creating listener sink: ${sink}")
    org.apache.spark.util.Utils.loadExtensions(
      classOf[SparkListenerSink],
      Seq(sink),
      conf).head
  }

  protected def logSparkListenerEvent(
                                       event: SparkListenerEvent,
                                       getTimestamp: () => Instant =
                                       () => Instant.now()): Unit = {
    val json = try {
      // Add a well-known time field.
      Some(
        JsonProtocol.sparkEventToJson(event)
          .merge(render(
            SparkInformation.get() + ("SparkEventTime" -> getTimestamp().toString)
          ))
      )
    } catch {
      case NonFatal(e) =>
        logError(s"Error serializing SparkListenerEvent to JSON: $event", e)
        None
    }

    sendToSink(json)
  }

  private[spark] def sendToSink(json: Option[JValue]): Unit = {
    try {
      json match {
        case Some(j) => {
          logDebug(s"Sending event to listener sink: ${compact(j)}")
          this.listenerSink.logEvent(json)
        }
        case None => {
          logWarning("json value was None")
        }
      }
    } catch {
      case NonFatal(e) =>
        logError(s"Error sending to listener sink: $e")
    }
  }
}

