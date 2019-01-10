package org.apache.spark.listeners

import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods.parse
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.mockito.Matchers.any
import org.scalatest.BeforeAndAfterEach

/**
  * This is more of behavior tester for LogAnalyticsListener implementation
  * that is extended from spark listener
  */
class LogAnalyticsListenerTester extends ListenerHelperSuite
  with BeforeAndAfterEach {

  implicit val defaultFormats = org.json4s.DefaultFormats

  private var conf: SparkConf = null

  private var listener: LogAnalyticsListener = null
  private var captor: ArgumentCaptor[Option[JValue]] = null

  override def beforeEach(): Unit = {
    super.beforeEach()
    val conf = new SparkConf()
    conf.set("spark.logAnalytics.workspaceId", "id")
    conf.set("spark.logAnalytics.secret", "secret")
    this.captor = ArgumentCaptor.forClass(classOf[Option[JValue]])
    this.listener = spy(new LogAnalyticsListener(conf))
    //when(listener.logEvent(any(classOf[Option[JValue]]))).thenReturn()
    doNothing.when(this.listener).logEvent(any(classOf[Option[JValue]]))
  }

  override def afterEach(): Unit = {
    super.afterEach()
    this.captor = null
    this.listener = null
  }

  override def beforeAll(): Unit = {
    conf = new SparkConf()
    conf.set("spark.logAnalytics.workspaceId", "id")
    conf.set("spark.logAnalytics.secret", "secret")
  }

  test("should invoke onStageSubmitted ") {

    val mockEvent = mock(classOf[SparkListenerStageSubmitted])
    this.listener.onStageSubmitted(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))
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


  // these  test  tests the lamda function for following cases
  // scenario 1 - event has time stamp
  // scenario 2 - event has timestamp field but could be optional. in that case instant.now needs to plugged
  // scenario 3 - event has no explicit time stamp field. in that case, default lambda in the definition
  // should be picked up and should result in instant.now value

  test("should invoke onBlockManagerAdded with serialized mock Event with lamda  ") {

    val event = SparkListenerBlockManagerAdded(
      SparkTestEvents.EPOCH_TIME,
      BlockManagerId.apply("driver", "localhost", 57967),
      278302556
    )

    this.listener.onBlockManagerAdded(event)
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

  test("onStageSubmitted with submission time optional empty should populate TimeGenerated") {

    val mockEvent = SparkTestEvents.sparkListenerStageSubmittedWithSubmissonTimeEmptyEvent
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onStageSubmitted(mockEvent)
    assert(sut.isLogEventInvoked)

    val timeGeneratedField = parse(sut.enrichedLogEvent).findField { case (n, v) => n == "TimeGenerated" }


    assert(timeGeneratedField.isDefined)
    assert(timeGeneratedField.get._1 == "TimeGenerated")

    assert(!timeGeneratedField.get._2.extract[String].isEmpty)


  }

  test("onStageSubmitted with submission time  should populate expected TimeGenerated") {

    val mockEvent = SparkTestEvents.sparkListenerStageSubmittedEvent
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onStageSubmitted(mockEvent)
    assert(sut.isLogEventInvoked)

    val timeGeneratedField = parse(sut.enrichedLogEvent).findField { case (n, v) => n == "TimeGenerated" }


    assert(timeGeneratedField.isDefined)
    assert(timeGeneratedField.get._1 == "TimeGenerated")


    assert(timeGeneratedField.get._2.extract[String].contentEquals(SparkTestEvents.EPOCH_TIME_AS_ISO8601))


  }

  test("onEnvironmentUpdate should populate instat.now timegenerated field") {

    val mockEvent = SparkTestEvents.sparkListenerEnvironmentUpdateEvent
    val sut = new LogAnalyticsListener(conf) with LogAnalyticsMock
    sut.onEnvironmentUpdate(mockEvent)
    assert(sut.isLogEventInvoked)

    val timeGeneratedField = parse(sut.enrichedLogEvent).findField { case (n, v) => n == "TimeGenerated" }


    assert(timeGeneratedField.isDefined)
    assert(timeGeneratedField.get._1 == "TimeGenerated")

    //implicit val formats = DefaultFormats
    assert(!timeGeneratedField.get._2.extract[String].isEmpty)
  }
}
