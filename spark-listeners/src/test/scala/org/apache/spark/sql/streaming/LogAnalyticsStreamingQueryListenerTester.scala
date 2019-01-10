package org.apache.spark.sql.streaming

import java.util.UUID

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.listeners.{LogAnalyticsStreamingQueryListener, SparkTestEvents}
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent
import org.json4s.JsonAST.JValue
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.any
import org.mockito.Mockito.{doNothing, mock, spy, verify}
import org.scalatest.BeforeAndAfterEach

class LogAnalyticsStreamingQueryListenerTester extends SparkFunSuite
  with BeforeAndAfterEach {

  implicit val defaultFormats = org.json4s.DefaultFormats

  private var listener: LogAnalyticsStreamingQueryListener = null
  private var captor: ArgumentCaptor[Option[JValue]] = null

  override def beforeEach(): Unit = {
    super.beforeEach()
    val conf = new SparkConf()
    conf.set("spark.logAnalytics.workspaceId", "id")
    conf.set("spark.logAnalytics.secret", "secret")
    this.captor = ArgumentCaptor.forClass(classOf[Option[JValue]])
    this.listener = spy(new LogAnalyticsStreamingQueryListener(conf))
    doNothing.when(this.listener).logEvent(any(classOf[Option[JValue]]))
  }

  override def afterEach(): Unit = {
    super.afterEach()
    this.captor = null
    this.listener = null
  }

  test("should invoke onQueryStarted ") {
    val event = mock(classOf[StreamingQueryListener.QueryStartedEvent])
    this.listener.onQueryStarted(event)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))
  }

  test("should invoke onQueryTerminated ") {
    val event = mock(classOf[StreamingQueryListener.QueryTerminatedEvent])
    this.listener.onQueryTerminated(event)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))
  }

  test("should invoke QueryProgressEvent ") {
    val event = mock(classOf[StreamingQueryListener.QueryProgressEvent])
    this.listener.onQueryProgress(event)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))
  }

  test("onQueryProgress with  time  should populate expected TimeGenerated") {

    import scala.collection.JavaConversions.mapAsJavaMap
    val event = new QueryProgressEvent(new StreamingQueryProgress(
      UUID.randomUUID,
      UUID.randomUUID,
      null,
      SparkTestEvents.EPOCH_TIME_AS_ISO8601,
      2L,
      mapAsJavaMap(Map("total" -> 0L)),
      mapAsJavaMap(Map.empty[String, String]),
      Array(new StateOperatorProgress(
        0, 1, 2)),
      Array(
        new SourceProgress(
          "source",
          "123",
          "456",
          678,
          Double.NaN,
          Double.NegativeInfinity
        )
      ),
      new SinkProgress("sink")
    ))

    this.listener.onQueryProgress(event)
    verify(this.listener).logEvent(this.captor.capture)
    this.captor.getValue match {
      case Some(jValue) => {
        jValue.findField { case (n, v) => n == "TimeGenerated" } match {
          case Some(t) => {
            assert(t._2.extract[String] == SparkTestEvents.EPOCH_TIME_AS_ISO8601)
          }
          case None => fail("TimeGenerated field not found")
        }
      }
      case None => fail("None passed to logEvent")
    }
  }
}
