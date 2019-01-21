package org.apache.spark.listeners

import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerEvent, SparkListenerExecutorRemoved, SparkListenerUnpersistRDD}
import org.apache.spark.streaming.scheduler.StreamingListenerEvent
import org.apache.spark.{LogAnalytics, SparkConf, SparkFunSuite}
import org.json4s.JsonAST.{JField, JValue}
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.any
import org.mockito.Mockito.{doNothing, mock, spy, verify}
import org.mockito.internal.util.MockUtil
import org.scalatest.BeforeAndAfterEach

import scala.reflect.ClassTag

class ListenerSuite[T <: LogAnalytics](implicit tag: ClassTag[T]) extends SparkFunSuite
  with BeforeAndAfterEach {

  protected implicit val defaultFormats = org.json4s.DefaultFormats

  protected var listener: T = null.asInstanceOf[T]
  private var logEventCaptor: ArgumentCaptor[Option[JValue]] = null

  val EPOCH_TIME = 1422981759407L
  val EPOCH_TIME_AS_ISO8601 = "2015-02-03T16:42:39.407Z"

  private val listenerFactory: (SparkConf) => T = conf => {
    tag.runtimeClass
      .getConstructor(classOf[SparkConf])
      .newInstance(conf)
      .asInstanceOf[T]
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    val conf = new SparkConf()
    conf.set("spark.logAnalytics.workspaceId", "id")
    conf.set("spark.logAnalytics.secret", "secret")
    this.logEventCaptor = ArgumentCaptor.forClass(classOf[Option[JValue]])
    this.listener = spy(this.listenerFactory(conf))
    doNothing.when(this.listener).logEvent(any(classOf[Option[JValue]]))
  }

  override def afterEach(): Unit = {
    super.afterEach()
    this.listener = null.asInstanceOf[T]
    this.logEventCaptor = null
  }

  protected def onSparkListenerEvent[T <: SparkListenerEvent](onEvent: T => Unit)(implicit tag: ClassTag[T]): Unit = {
    val event = mock(tag.runtimeClass).asInstanceOf[T]
    this.onSparkListenerEvent(onEvent, event)
  }

  val mockUtil = new MockUtil();

  // for mocked Spark listener events , serialization fails except  for the mocks of SparkListenerUnpersistRDD,
  //SparkListenerApplicationEnd,SparkListenerExecutorRemoved . in these failure cases , log event is invoked with None.
  protected def onSparkListenerEvent[T <: SparkListenerEvent](onEvent: T => Unit, event: T): Option[JValue] = {
    onEvent(event)
    verify(this.listener).logEvent(this.logEventCaptor.capture)

    val capturedValue = this.logEventCaptor.getValue
    if (mockUtil.isMock(event)
      && !event.isInstanceOf[SparkListenerUnpersistRDD]
      && !event.isInstanceOf[SparkListenerApplicationEnd]
      && !event.isInstanceOf[SparkListenerExecutorRemoved]) {
      assert(capturedValue.isEmpty)
    }
    else {
      assert(capturedValue.isDefined)
    }
    capturedValue
  }

  protected def onStreamingListenerEvent[T <: StreamingListenerEvent](onEvent: T => Unit)(implicit tag: ClassTag[T]): Unit = {
    val event = mock(tag.runtimeClass).asInstanceOf[T]
    this.onStreamingListenerEvent(onEvent, event)
  }

  // for mocked Streaming listener events, serialization fails. In that case log event is invoked with none
  protected def onStreamingListenerEvent[T <: StreamingListenerEvent](onEvent: T => Unit, event: T): Option[JValue] = {
    onEvent(event)
    verify(this.listener).logEvent(this.logEventCaptor.capture)
    val capturedValue = this.logEventCaptor.getValue
    if (mockUtil.isMock(event)) {
      assert(capturedValue.isEmpty)
    }
    else {
      assert(capturedValue.isDefined)
    }
    capturedValue
  }

  protected def assertSparkEventTime(
                                      json: Option[JValue],
                                      assertion: (JField) => org.scalatest.Assertion): org.scalatest.Assertion =
    this.assertField(json, "SparkEventTime", assertion)
//  protected def assertSparkEventTime(json: Option[JValue], assertion: (JField) => org.scalatest.Assertion) = {
//    json match {
//      case Some(jValue) => {
//        jValue.findField { case (n, _) => n == "SparkEventTime" } match {
//          case Some(jField) => {
//            assertion(jField)
//          }
//          case None => fail("SparkEventTime field not found")
//        }
//      }
//      case None => fail("None passed to logEvent")
//    }
//  }

  protected def assertField(
                             json: Option[JValue],
                             fieldName: String,
                             assertion: (JField) => org.scalatest.Assertion): org.scalatest.Assertion = {
    json match {
      case Some(jValue) => {
        jValue.findField { case (n, _) => n == fieldName } match {
          case Some(jField) => {
            assertion(jField)
          }
          case None => fail(s"${fieldName} field not found")
        }
      }
      case None => fail("None passed to logEvent")
    }
  }
}
