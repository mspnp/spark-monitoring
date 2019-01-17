package org.apache.spark.sql.streaming

import java.util.UUID

import org.apache.spark.listeners.{ListenerSuite, LogAnalyticsListener}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.json4s.JsonAST.JValue
import org.mockito.Matchers.any
import org.mockito.Mockito.{mock, times, verify, when}
import org.scalatest.BeforeAndAfterEach

class LogAnalyticsListenerWithStreamingQueryEventsSuite extends ListenerSuite[LogAnalyticsListener]
  with BeforeAndAfterEach {


  test("should not logevent when  invoked on onOtherEvent with event of type StreamingQueryProgressEvent ") {
    import scala.collection.JavaConversions.mapAsJavaMap
    val event = new QueryProgressEvent(new StreamingQueryProgress(
      UUID.randomUUID,
      UUID.randomUUID,
      null,
      EPOCH_TIME_AS_ISO8601,
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

    this.listener.onOtherEvent(event)
    verify(this.listener, times(0)).logEvent(any(classOf[Option[JValue]]))
  }

  test("should not logevent when  invoked on onOtherEvent with event of type StreamingQueryStartedEvent") {
    val event = mock(classOf[QueryStartedEvent])
    when(event.logEvent).thenReturn(true)
    this.listener.onOtherEvent(event)
    verify(this.listener, times(0)).logEvent(any(classOf[Option[JValue]]))
  }

  test("should not logevent when  invoked on onOtherEvent with event of type StreamingQueryTerminatedEvent") {
    val event = mock(classOf[QueryTerminatedEvent])
    when(event.logEvent).thenReturn(false)
    this.listener.onOtherEvent(event)
    verify(this.listener, times(0)).logEvent(any(classOf[Option[JValue]]))
  }
}
