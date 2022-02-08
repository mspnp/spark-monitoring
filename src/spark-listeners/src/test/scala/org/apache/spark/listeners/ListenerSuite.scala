package org.apache.spark.listeners

import org.apache.spark.internal.Logging
import org.apache.spark.listeners.sink.SparkListenerSink
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.streaming.scheduler.StreamingListenerEvent
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.json4s.JsonAST.JValue
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{spy, times, verify}
import org.scalatest.BeforeAndAfterEach

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

class TestSparkListenerSink extends SparkListenerSink with Logging {
  override def logEvent(event: Option[JValue]): Unit = {
    logInfo(s"sendToSink called: ${event}")
  }
}

object ListenerSuite {
  val EPOCH_TIME = 1422981759407L
  val EPOCH_TIME_AS_ISO8601 = "2015-02-03T16:42:39.407Z"
}

class ListenerSuite extends SparkFunSuite
  with BeforeAndAfterEach {

  protected implicit val defaultFormats = org.json4s.DefaultFormats
  protected var listener: UnifiedSparkListener = null
  private var logEventCaptor: ArgumentCaptor[Option[JValue]] = null

  override def beforeEach(): Unit = {
    super.beforeEach()
    // We will use a mock sink
    val conf = new SparkConf()
        .set("spark.driver.allowMultipleContexts", "true")
        .set("spark.unifiedListener.sink", classOf[TestSparkListenerSink].getName)
    this.logEventCaptor = ArgumentCaptor.forClass(classOf[Option[JValue]])
    this.listener = spy(new UnifiedSparkListener(conf))
  }

  override def afterEach(): Unit = {
    super.afterEach()
    this.listener = null
    this.logEventCaptor = null
  }

  protected def onSparkListenerEvent[T <: SparkListenerEvent](
                                                               onEvent: T => Unit,
                                                               event: T): (Option[JValue], T) = {
    onEvent(event)
    verify(this.listener, times(1)).sendToSink(this.logEventCaptor.capture)
    (
      this.logEventCaptor.getValue,
      event
    )
  }

  private val wrapperCtor: ru.MethodMirror = {
    // We need to get the wrapper class so we can wrap this the way Spark does
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val streamingListenerBusClassSymbol = mirror.classSymbol(
      Class.forName("org.apache.spark.streaming.scheduler.StreamingListenerBus")
    )

    val streamingListenerBusClassMirror = mirror.reflectClass(streamingListenerBusClassSymbol)
    val streamingListenerBusCtor = streamingListenerBusClassMirror
      .reflectConstructor(
        streamingListenerBusClassSymbol.typeSignature.members.filter(_.isConstructor).head.asMethod
      )
    val streamingListenerBus = streamingListenerBusCtor(null)
    val streamingListenerBusInstanceMirror = mirror.reflect(streamingListenerBus)

    val wrappedStreamingListenerEventClassSymbol = mirror.classSymbol(
      Class.forName(
        StreamingListenerHandlers.WrappedStreamingListenerEventClassName
      )
    )

    val wrappedStreamingListenerEventClassSymbolCtor = wrappedStreamingListenerEventClassSymbol
      .typeSignature.members.filter(_.isConstructor).head.asMethod
    streamingListenerBusInstanceMirror.reflectClass(
      wrappedStreamingListenerEventClassSymbol
    ).reflectConstructor(wrappedStreamingListenerEventClassSymbolCtor)
  }

  // All StreamingListenerEvents go through the onOtherEvent method, so we will call directly here.
  protected def onStreamingListenerEvent[T <: StreamingListenerEvent](event: T): (Option[JValue], T) = {
    // This one is the odd one.
    val (json, _) = onSparkListenerEvent(
      this.listener.onOtherEvent,
      this.wrapperCtor.apply(event).asInstanceOf[SparkListenerEvent]
    )
    (
      json,
      event
    )
  }

  protected def onStreamingQueryListenerEvent[T <: StreamingQueryListener.Event](
                                                                                  event: T): (Option[JValue], T) = {
    onSparkListenerEvent(
      this.listener.onOtherEvent,
      event
    )
  }

  protected def assertEvent[T <: AnyRef](
                                          json: Option[JValue],
                                          event: T)(implicit classTag: ClassTag[T]): org.scalatest.Assertion = {
    this.assertField(
      json,
      "Event",
      (_, value) => assert(value.extract[String] === classTag.runtimeClass.getName)
    )
  }

  protected def assertSparkEventTime(
                                      json: Option[JValue],
                                      assertion: (String, JValue) => org.scalatest.Assertion): org.scalatest.Assertion =
    this.assertField(json, "SparkEventTime", assertion)

  protected def assertField(
                             json: Option[JValue],
                             fieldName: String,
                             assertion: (String, JValue) => org.scalatest.Assertion): org.scalatest.Assertion = {
    json match {
      case Some(jValue) => {
        jValue.findField { case (n, _) => n == fieldName } match {
          case Some(jField) => {
            assertion.tupled(jField)
          }
          case None => fail(s"${fieldName} field not found")
        }
      }
      case None => fail("None passed to assertField")
    }
  }
}
