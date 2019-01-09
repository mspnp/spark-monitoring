package org.apache.spark.listeners

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.mockito.Mockito.mock

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

}
