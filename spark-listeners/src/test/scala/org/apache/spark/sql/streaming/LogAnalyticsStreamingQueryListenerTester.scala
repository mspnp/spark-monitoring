package org.apache.spark.sql.streaming

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.listeners.SparkTestEvents.iso8601TestTime
import org.apache.spark.listeners.{ListenerHelperSuite, LogAnalyticsStreamingQueryListener, SparkTestEvents}
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.mockito.Mockito.mock

import scala.collection.JavaConverters._

/**
  * This is more of behavior tester for LogAnalyticsStreamingQueryListener implementation
  * that is extended from StreamingQueryListener
  */
class LogAnalyticsStreamingQueryListenerTester extends ListenerHelperSuite {

  private var conf: SparkConf = null

  override def beforeAll(): Unit = {
    conf = new SparkConf()
    conf.set("spark.logAnalytics.workspaceId", "id")
    conf.set("spark.logAnalytics.secret", "secret")
  }

  test("should invoke onQueryStarted ") {

    val mockEvent = mock(classOf[StreamingQueryListener.QueryStartedEvent])
    val sut = new LogAnalyticsStreamingQueryListener(conf) with LogAnalyticsMock
    sut.onQueryStarted(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onQueryTerminated ") {

    val mockEvent = mock(classOf[StreamingQueryListener.QueryTerminatedEvent])
    val sut = new LogAnalyticsStreamingQueryListener(conf) with LogAnalyticsMock
    sut.onQueryTerminated(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke QueryProgressEvent ") {

    val mockEvent = mock(classOf[StreamingQueryListener.QueryProgressEvent])
    val sut = new LogAnalyticsStreamingQueryListener(conf) with LogAnalyticsMock
    sut.onQueryProgress(mockEvent)
    assert(sut.isLogEventInvoked)

  }


  // these  test  tests the lamda function for following cases
  test("onQueryProgress with  time  should populate expected TimeGenerated") {

    val queryProgress = new StreamingQueryProgress(
      UUID.randomUUID,
      UUID.randomUUID,
      null,
      iso8601TestTime,
      2L,
      new java.util.HashMap(Map("total" -> 0L).mapValues(long2Long).asJava),
      new java.util.HashMap(Map.empty[String, String].asJava),
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
    )

    val mockEvent = new QueryProgressEvent(queryProgress)
    val sut = new LogAnalyticsStreamingQueryListener(conf) with LogAnalyticsMock
    sut.onQueryProgress(mockEvent)
    assert(sut.isLogEventInvoked)

    val timeGeneratedField = parse(sut.enrichedLogEvent).findField { case (n, v) => n == "TimeGenerated" }


    assert(timeGeneratedField.isDefined)
    assert(timeGeneratedField.get._1 == "TimeGenerated")


    implicit val formats = DefaultFormats
    assert(timeGeneratedField.get._2.extract[String].contentEquals(SparkTestEvents.iso8601TestTime))


  }

}
