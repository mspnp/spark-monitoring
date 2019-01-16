package org.apache.spark.listeners

import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.streaming.scheduler.StreamingListenerEvent
import org.apache.spark.{LogAnalytics, SparkConf, SparkFunSuite}
import org.json4s.JsonAST.{JField, JValue}
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.any
import org.mockito.Mockito.{doNothing, mock, spy, verify}
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

  protected def onSparkListenerEvent[T <: SparkListenerEvent](onEvent: T => Unit, event: T): Option[JValue] = {
    onEvent(event)
    verify(this.listener).logEvent(this.logEventCaptor.capture)
    this.logEventCaptor.getValue
  }

  protected def onStreamingListenerEvent[T <: StreamingListenerEvent](onEvent: T => Unit)(implicit tag: ClassTag[T]): Unit = {
    val event = mock(tag.runtimeClass).asInstanceOf[T]
    this.onStreamingListenerEvent(onEvent, event)
  }

  protected def onStreamingListenerEvent[T <: StreamingListenerEvent](onEvent: T => Unit, event: T): Option[JValue] = {
    onEvent(event)
    verify(this.listener).logEvent(this.logEventCaptor.capture)
    this.logEventCaptor.getValue
  }

  protected def assertSparkEventTime(json: Option[JValue], assertion: (JField) => org.scalatest.Assertion) = {
    json match {
      case Some(jValue) => {
        jValue.findField { case (n, _) => n == "SparkEventTime" } match {
          case Some(jField) => {
            assertion(jField)
          }
          case None => fail("SparkEventTime field not found")
        }
      }
      case None => fail("None passed to logEvent")
    }
  }
}
