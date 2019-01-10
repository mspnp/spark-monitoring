package org.apache.spark.listeners

import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId
import org.json4s.JsonAST.JValue
import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, Mockito}
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
    this.listener.onTaskStart(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))
  }

  test("should invoke onTaskGettingResult ") {

    val mockEvent = mock(classOf[SparkListenerTaskGettingResult])
    this.listener.onTaskGettingResult(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should invoke onTaskEnd ") {

    val mockEvent = mock(classOf[SparkListenerTaskEnd])
    this.listener.onTaskEnd(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should invoke onEnvironmentUpdate ") {

    val mockEvent = mock(classOf[SparkListenerEnvironmentUpdate])
    when(mockEvent.environmentDetails).thenReturn(Map("someKey" -> Seq(("tuple1-1", "tuple1-2"), ("tuple2-1", "tuple2-2"))))
    this.listener.onEnvironmentUpdate(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should invoke onStageCompleted ") {

    val mockEvent = mock(classOf[SparkListenerStageCompleted])
    this.listener.onStageCompleted(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should invoke onJobStart ") {

    val mockEvent = mock(classOf[SparkListenerJobStart])
    this.listener.onJobStart(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should invoke onJobEnd ") {

    val mockEvent = mock(classOf[SparkListenerJobEnd])
    this.listener.onJobEnd(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }


  test("should invoke onBlockManagerAdded ") {

    val mockEvent = mock(classOf[SparkListenerBlockManagerAdded])
    this.listener.onBlockManagerAdded(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should invoke onBlockManagerRemoved ") {

    val mockEvent = mock(classOf[SparkListenerBlockManagerRemoved])
    this.listener.onBlockManagerRemoved(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should invoke onUnpersistRDD ") {

    val mockEvent = mock(classOf[SparkListenerUnpersistRDD])
    this.listener.onUnpersistRDD(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should invoke onApplicationStart ") {

    val mockEvent = mock(classOf[SparkListenerApplicationStart])
    this.listener.onApplicationStart(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should invoke onApplicationEnd ") {

    val mockEvent = mock(classOf[SparkListenerApplicationEnd])
    this.listener.onApplicationEnd(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should invoke onExecutorAdded ") {

    val mockEvent = mock(classOf[SparkListenerExecutorAdded])
    this.listener.onExecutorAdded(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }
  test("should invoke onExecutorRemoved ") {

    val mockEvent = mock(classOf[SparkListenerExecutorRemoved])
    this.listener.onExecutorRemoved(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should invoke onExecutorBlacklisted ") {

    val mockEvent = mock(classOf[SparkListenerExecutorBlacklisted])
    this.listener.onExecutorBlacklisted(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should invoke onNodeBlacklisted ") {

    val mockEvent = mock(classOf[SparkListenerNodeBlacklisted])
    this.listener.onNodeBlacklisted(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should invoke onNodeUnblacklisted ") {

    val mockEvent = mock(classOf[SparkListenerNodeUnblacklisted])
    this.listener.onNodeUnblacklisted(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should not invoke onBlockUpdated when logBlockUpdates is set to false ") {

    val mockEvent = mock(classOf[SparkListenerBlockUpdated])
    this.listener.onBlockUpdated(mockEvent)
    verify(this.listener, Mockito.times(0)).logEvent(any(classOf[Option[JValue]]))

  }

  test("should  invoke onBlockUpdated when logBlockUpdates is set to true ") {

    conf.set("spark.logAnalytics.logBlockUpdates", "true")
    this.listener = spy(new LogAnalyticsListener(conf))
    val mockEvent = mock(classOf[SparkListenerBlockUpdated])
    this.listener.onBlockUpdated(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should onExecutorMetricsUpdate be always no op") {

    val mockEvent = mock(classOf[SparkListenerExecutorMetricsUpdate])
    this.listener.onExecutorMetricsUpdate(mockEvent)
    verify(this.listener, Mockito.times(0)).logEvent(any(classOf[Option[JValue]]))
  }

  test("should invoke onOtherEvent but don't log if logevent is not enabled ") {

    val mockEvent = mock(classOf[SparkListenerEvent])
    when(mockEvent.logEvent).thenReturn(false)
    this.listener.onOtherEvent(mockEvent)
    verify(this.listener, Mockito.times(0)).logEvent(any(classOf[Option[JValue]]))
  }

  test("should  invoke onOtherEvent but will log logevent is  enabled ") {

    val mockEvent = mock(classOf[SparkListenerEvent])
    when(mockEvent.logEvent).thenReturn(true)
    this.listener.onOtherEvent(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))
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
    this.listener.onStageSubmitted(mockEvent)
    verify(this.listener).logEvent(this.captor.capture)

    this.captor.getValue match {
      case Some(jValue) => {
        jValue.findField { case (n, v) => n == "TimeGenerated" } match {
          case Some(t) => {
            assert(!t._2.extract[String].isEmpty)
          }
          case None => fail("TimeGenerated field not found")
        }
      }
      case None => fail("None passed to logEvent")
    }


  }

  test("onStageSubmitted with submission time  should populate expected TimeGenerated") {

    val mockEvent = SparkTestEvents.sparkListenerStageSubmittedEvent
    this.listener.onStageSubmitted(mockEvent)
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

  test("onEnvironmentUpdate should populate instat.now timegenerated field") {

    val mockEvent = SparkTestEvents.sparkListenerEnvironmentUpdateEvent
    this.listener.onEnvironmentUpdate(mockEvent)
    verify(this.listener).logEvent(this.captor.capture)

    this.captor.getValue match {
      case Some(jValue) => {
        jValue.findField { case (n, v) => n == "TimeGenerated" } match {
          case Some(t) => {
            assert(!t._2.extract[String].isEmpty)
          }
          case None => fail("TimeGenerated field not found")
        }
      }
      case None => fail("None passed to logEvent")
    }
  }
}
