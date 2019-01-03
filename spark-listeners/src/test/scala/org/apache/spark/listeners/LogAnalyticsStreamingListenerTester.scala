package org.apache.spark.listeners

import org.apache.spark.SparkConf
import org.apache.spark.streaming.scheduler._
import org.mockito.Mockito.mock


/**
  * Since extended Streaming listeners override ,these test are more of behavioral validation.
  * if overriding certain methods in LogAnalyticsStreamingListener change
  * then come , validate and change here
  */
class LogAnalyticsStreamingListenerTester extends ListenerHelperSuite {


  private var conf: SparkConf = null

  override def beforeAll(): Unit = {
    conf = new SparkConf()
    conf.set("spark.logAnalytics.workspaceId", "id")
    conf.set("spark.logAnalytics.secret", "secret")
  }


  test("should invoke onStreamingStarted ") {

    val mockEvent = mock(classOf[StreamingListenerStreamingStarted])
    val sut = new LogAnalyticsStreamingListener(conf) with LogAnalyticsMock
    sut.onStreamingStarted(mockEvent)
    assert(sut.isLogEventInvoked)

  }


  test("should invoke onReceiverStarted ") {

    val mockEvent = mock(classOf[StreamingListenerReceiverStarted])
    val sut = new LogAnalyticsStreamingListener(conf) with LogAnalyticsMock
    sut.onReceiverStarted(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onReceiverError ") {

    val mockEvent = mock(classOf[StreamingListenerReceiverError])
    val sut = new LogAnalyticsStreamingListener(conf) with LogAnalyticsMock
    sut.onReceiverError(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onReceiverStopped ") {

    val mockEvent = mock(classOf[StreamingListenerReceiverStopped])
    val sut = new LogAnalyticsStreamingListener(conf) with LogAnalyticsMock
    sut.onReceiverStopped(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onBatchSubmitted ") {

    val mockEvent = mock(classOf[StreamingListenerBatchSubmitted])
    val sut = new LogAnalyticsStreamingListener(conf) with LogAnalyticsMock
    sut.onBatchSubmitted(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onBatchStarted ") {

    val mockEvent = mock(classOf[StreamingListenerBatchStarted])
    val sut = new LogAnalyticsStreamingListener(conf) with LogAnalyticsMock
    sut.onBatchStarted(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onBatchCompleted ") {

    val mockEvent = mock(classOf[StreamingListenerBatchCompleted])
    val sut = new LogAnalyticsStreamingListener(conf) with LogAnalyticsMock
    sut.onBatchCompleted(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onOutputOperationStarted ") {

    val mockEvent = mock(classOf[StreamingListenerOutputOperationStarted])
    val sut = new LogAnalyticsStreamingListener(conf) with LogAnalyticsMock
    sut.onOutputOperationStarted(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onOutputOperationCompleted ") {

    val mockEvent = mock(classOf[StreamingListenerOutputOperationCompleted])
    val sut = new LogAnalyticsStreamingListener(conf) with LogAnalyticsMock
    sut.onOutputOperationCompleted(mockEvent)
    assert(sut.isLogEventInvoked)

  }



}
