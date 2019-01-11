package org.apache.spark.sql.streaming

import java.util.UUID
import org.apache.spark.listeners.{ListenerSuite, LogAnalyticsStreamingQueryListener}
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent
import org.scalatest.BeforeAndAfterEach

class LogAnalyticsStreamingQueryListenerSuite extends ListenerSuite[LogAnalyticsStreamingQueryListener] ///SparkFunSuite
  with BeforeAndAfterEach {

  test("should invoke onQueryStarted ") {
    this.onSparkListenerEvent(this.listener.onQueryStarted)
  }

  test("should invoke onQueryTerminated ") {
    this.onSparkListenerEvent(this.listener.onQueryTerminated)
  }

  test("should invoke QueryProgressEvent ") {
    this.onSparkListenerEvent(this.listener.onQueryProgress)
  }

  test("onQueryProgress with  time  should populate expected TimeGenerated") {

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

    this.assertTimeGenerated(
      this.onSparkListenerEvent(this.listener.onQueryProgress, event),
      t => assert(t._2.extract[String] == EPOCH_TIME_AS_ISO8601)
    )
  }
}
