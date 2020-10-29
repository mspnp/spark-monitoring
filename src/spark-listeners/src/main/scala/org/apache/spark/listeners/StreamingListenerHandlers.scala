package org.apache.spark.listeners

import java.time.Instant

import com.fasterxml.jackson.annotation.{JsonTypeInfo, JsonUnwrapped}
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver
import com.fasterxml.jackson.databind.{DatabindContext, JavaType}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.streaming.scheduler._

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

private object StreamingListenerHandlers {
  val WrappedStreamingListenerEventClassName: String =
    "org.apache.spark.streaming.scheduler.StreamingListenerBus$WrappedStreamingListenerEvent"
  val StreamingListenerEventFieldName: String = "streamingListenerEvent"
}

trait StreamingListenerHandlers {
  this: UnifiedSparkListenerHandler with Logging =>

  private val wrappedStreamingListenerEventClass: Class[_] = {
    // This is the class Spark uses internally to wrap StreamingListenerEvents so it can pass through
    // the LiveListenerBus.  It's private, so we'll get it this way.  If this class name ever changes,
    // this code needs to be updated!!!
    val clazz = Class.forName(StreamingListenerHandlers.WrappedStreamingListenerEventClassName)
    if (clazz == null) {
      throw new ClassNotFoundException(
        s"Error loading class: ${StreamingListenerHandlers.WrappedStreamingListenerEventClassName}")
    }
    clazz
  }

  private val streamingListenerEventField: ru.TermSymbol = {
    val classSymbol = ru.runtimeMirror(
      this.wrappedStreamingListenerEventClass.getClassLoader
    ).classSymbol(this.wrappedStreamingListenerEventClass)
    classSymbol.typeSignature.member(ru.TermName(
      StreamingListenerHandlers.StreamingListenerEventFieldName)
    ) match {
      case symbol: ru.SymbolApi => symbol.asTerm
      case null => throw new NoSuchFieldException(
        s"Error reflecting ${StreamingListenerHandlers.StreamingListenerEventFieldName} field")
    }
  }

  protected val streamingListenerEventClassTag: ClassTag[SparkListenerEvent] =
    ClassTag[SparkListenerEvent](this.wrappedStreamingListenerEventClass)

  import scala.language.implicitConversions

  // Unwrap the StreamingListenerEvent from the Spark-owned private, inner class
  protected implicit def wrappedStreamingListenerEventToStreamingListenerEvent(event: SparkListenerEvent): Option[StreamingListenerEvent] = {
    val instanceMirror: ru.InstanceMirror = ru.runtimeMirror(
      event.getClass.getClassLoader
    ).reflect(event)

    val fieldMirror = instanceMirror.reflectField(streamingListenerEventField)
    Some(fieldMirror.get.asInstanceOf[StreamingListenerEvent])
  }

  // Re-wrap the StreamingListenerEvent in our wrapper.  The wrapper contains the JSON
  // serialization bits to serialize StreamingListenerEvents properly so we don't have to
  // maintain our own ObjectMapper
  private implicit def streamingListenerEventToSparkListenerEvent(event: StreamingListenerEvent): SparkListenerEvent = {
    new StreamingListenerEventWrapper(event)
  }

  protected def onStreamingListenerEvent(event: Option[StreamingListenerEvent]): Unit = {
    event match {
      case Some(sle) => sle match {
        case streamingStarted: StreamingListenerStreamingStarted =>
          this.logSparkListenerEvent(
            streamingStarted,
            () => Instant.ofEpochMilli(streamingStarted.time)
          )
        case batchCompleted: StreamingListenerBatchCompleted =>
          this.logSparkListenerEvent(
            batchCompleted,
            () => Instant.ofEpochMilli(
              batchCompleted.batchInfo.processingEndTime.getOrElse(
                Instant.now.toEpochMilli
              )
            )
          )
        case batchStarted: StreamingListenerBatchStarted =>
          this.logSparkListenerEvent(
            batchStarted,
            () => Instant.ofEpochMilli(
              batchStarted.batchInfo.processingStartTime.getOrElse(
                Instant.now().toEpochMilli
              )
            )
          )
        case batchSubmitted: StreamingListenerBatchSubmitted =>
          this.logSparkListenerEvent(
            batchSubmitted,
            () => Instant.ofEpochMilli(batchSubmitted.batchInfo.submissionTime)
          )
        case outputOperationCompleted: StreamingListenerOutputOperationCompleted =>
          this.logSparkListenerEvent(
            outputOperationCompleted,
            () => Instant.ofEpochMilli(
              outputOperationCompleted.outputOperationInfo.endTime.getOrElse(
                Instant.now.toEpochMilli
              )
            )
          )
        case outputOperationStarted: StreamingListenerOutputOperationStarted =>
          this.logSparkListenerEvent(
            outputOperationStarted,
            () => Instant.ofEpochMilli(
              outputOperationStarted.outputOperationInfo.startTime.getOrElse(
                Instant.now.toEpochMilli
              )
            )
          )
        case receiverError: StreamingListenerReceiverError =>
          this.logSparkListenerEvent(receiverError)
        case receiverStarted: StreamingListenerReceiverStarted =>
          this.logSparkListenerEvent(receiverStarted)
        case receiverStopped: StreamingListenerReceiverStopped =>
          this.logSparkListenerEvent(receiverStopped)
      }
      case None => this.logWarning("StreamingListenerEvent was None")
    }
  }
}

@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = JsonTypeInfo.As.PROPERTY, property = "Event")
@JsonTypeIdResolver(classOf[StreamingListenerEventWrapperTypeIdResolver])
class StreamingListenerEventWrapper(
                                     @JsonUnwrapped
                                     val streamingListenerEvent: StreamingListenerEvent
                                   ) extends SparkListenerEvent

class StreamingListenerEventWrapperTypeIdResolver extends com.fasterxml.jackson.databind.jsontype.TypeIdResolver {
  private var javaType: JavaType = _

  override def init(javaType: JavaType): Unit = {
    this.javaType = javaType
  }

  override def idFromValue(o: Any): String = this.idFromValueAndType(o, o.getClass)

  override def idFromValueAndType(o: Any, aClass: Class[_]): String = {
    o
      .asInstanceOf[StreamingListenerEventWrapper]
      .streamingListenerEvent
      .getClass
      .getName
  }

  override def idFromBaseType(): String = throw new NotImplementedError()

  def typeFromId(s: String): JavaType = throw new NotImplementedError()

  override def typeFromId(databindContext: DatabindContext, s: String): JavaType = throw new NotImplementedError()

  override def getMechanism: JsonTypeInfo.Id = JsonTypeInfo.Id.CUSTOM

  def getDescForKnownTypeIds: String = throw new NotImplementedError()
}
