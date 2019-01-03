package org.apache.spark.listeners

import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.mockito.Mockito.{mock, when}

/**
  * This is more of behavior tester for LogAnalyticsListener implementation
  * that is extended from spark listener
  */
class LogAnalyticsListenerTester extends ListenerHelperSuite {

  private var conf: SparkConf = null

  override def beforeAll(): Unit = {
    conf = new SparkConf()
    conf.set("spark.logAnalytics.workspaceId", "id")
    conf.set("spark.logAnalytics.secret", "secret")
  }

  test("should invoke onStageSubmitted ") {

    val mockEvent = mock(classOf[SparkListenerStageSubmitted])
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onStageSubmitted(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onTaskStart ") {

    val mockEvent = mock(classOf[SparkListenerTaskStart])
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onTaskStart(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onTaskGettingResult ") {

    val mockEvent = mock(classOf[SparkListenerTaskGettingResult])
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onTaskGettingResult(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onTaskEnd ") {

    val mockEvent = mock(classOf[SparkListenerTaskEnd])
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onTaskEnd(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onEnvironmentUpdate ") {

    val mockEvent = mock(classOf[SparkListenerEnvironmentUpdate])
    when(mockEvent.environmentDetails).thenReturn(Map("someKey" -> Seq(("tuple1-1", "tuple1-2"), ("tuple2-1", "tuple2-2"))))
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onEnvironmentUpdate(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onStageCompleted ") {

    val mockEvent = mock(classOf[SparkListenerStageCompleted])
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onStageCompleted(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onJobStart ") {

    val mockEvent = mock(classOf[SparkListenerJobStart])
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onJobStart(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onJobEnd ") {

    val mockEvent = mock(classOf[SparkListenerJobEnd])
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onJobEnd(mockEvent)
    assert(sut.isLogEventInvoked)

  }


  test("should invoke onBlockManagerAdded ") {

    val mockEvent = mock(classOf[SparkListenerBlockManagerAdded])
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onBlockManagerAdded(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onBlockManagerRemoved ") {

    val mockEvent = mock(classOf[SparkListenerBlockManagerRemoved])
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onBlockManagerRemoved(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onUnpersistRDD ") {

    val mockEvent = mock(classOf[SparkListenerUnpersistRDD])
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onUnpersistRDD(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onApplicationStart ") {

    val mockEvent = mock(classOf[SparkListenerApplicationStart])
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onApplicationStart(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onApplicationEnd ") {

    val mockEvent = mock(classOf[SparkListenerApplicationEnd])
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onApplicationEnd(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onExecutorAdded ") {

    val mockEvent = mock(classOf[SparkListenerExecutorAdded])
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onExecutorAdded(mockEvent)
    assert(sut.isLogEventInvoked)

  }
  test("should invoke onExecutorRemoved ") {

    val mockEvent = mock(classOf[SparkListenerExecutorRemoved])
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onExecutorRemoved(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onExecutorBlacklisted ") {

    val mockEvent = mock(classOf[SparkListenerExecutorBlacklisted])
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onExecutorBlacklisted(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onNodeBlacklisted ") {

    val mockEvent = mock(classOf[SparkListenerNodeBlacklisted])
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onNodeBlacklisted(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should invoke onNodeUnblacklisted ") {

    val mockEvent = mock(classOf[SparkListenerNodeUnblacklisted])
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onNodeUnblacklisted(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should not invoke onBlockUpdated when logBlockUpdates is set to false ") {

    val mockEvent = mock(classOf[SparkListenerBlockUpdated])
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onBlockUpdated(mockEvent)
    assert(!sut.isLogEventInvoked)

  }

  test("should  invoke onBlockUpdated when logBlockUpdates is set to true ") {

    conf.set("spark.logAnalytics.logBlockUpdates", "true")
    val mockEvent = mock(classOf[SparkListenerBlockUpdated])
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onBlockUpdated(mockEvent)
    assert(sut.isLogEventInvoked)

  }

  test("should onExecutorMetricsUpdate be always no op") {

    val mockEvent = mock(classOf[SparkListenerExecutorMetricsUpdate])
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onExecutorMetricsUpdate(mockEvent)
    assert(!sut.isLogEventInvoked)
  }

  test("should invoke onOtherEvent but don't log if logevent is not enabled ") {

    val mockEvent = mock(classOf[SparkListenerEvent])
    when(mockEvent.logEvent).thenReturn(false)
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onOtherEvent(mockEvent)
    assert(!sut.isLogEventInvoked)
  }

  test("should  invoke onOtherEvent but will log logevent is  enabled ") {

    val mockEvent = mock(classOf[SparkListenerEvent])
    when(mockEvent.logEvent).thenReturn(true)
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onOtherEvent(mockEvent)
    assert(sut.isLogEventInvoked)
  }
}
