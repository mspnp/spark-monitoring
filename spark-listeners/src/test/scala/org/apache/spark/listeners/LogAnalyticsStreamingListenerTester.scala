package org.apache.spark.listeners

import org.apache.spark.SparkConf
import org.apache.spark.streaming.scheduler._
import org.json4s.JsonAST.JValue
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.any
import org.mockito.Mockito.{doNothing, mock, spy, verify}
import org.scalatest.BeforeAndAfterEach


/**
  * Since extended Streaming listeners override ,these test are more of behavioral validation.
  * if overriding certain methods in LogAnalyticsStreamingListener change
  * then come , validate and change here
  */
class LogAnalyticsStreamingListenerTester extends ListenerHelperSuite
  with BeforeAndAfterEach {

  implicit val defaultFormats = org.json4s.DefaultFormats

  private var conf: SparkConf = null

  private var listener: LogAnalyticsStreamingListener = null
  private var captor: ArgumentCaptor[Option[JValue]] = null

  override def beforeEach(): Unit = {
    super.beforeEach()
    val conf = new SparkConf()
    conf.set("spark.logAnalytics.workspaceId", "id")
    conf.set("spark.logAnalytics.secret", "secret")
    this.captor = ArgumentCaptor.forClass(classOf[Option[JValue]])
    this.listener = spy(new LogAnalyticsStreamingListener(conf))
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


  test("should invoke onStreamingStarted ") {

    val mockEvent = mock(classOf[StreamingListenerStreamingStarted])
    this.listener.onStreamingStarted(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }


  test("should invoke onReceiverStarted ") {

    val mockEvent = mock(classOf[StreamingListenerReceiverStarted])
    this.listener.onReceiverStarted(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should invoke onReceiverError ") {

    val mockEvent = mock(classOf[StreamingListenerReceiverError])
    this.listener.onReceiverError(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should invoke onReceiverStopped ") {

    val mockEvent = mock(classOf[StreamingListenerReceiverStopped])
    this.listener.onReceiverStopped(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should invoke onBatchSubmitted ") {

    val mockEvent = mock(classOf[StreamingListenerBatchSubmitted])
    this.listener.onBatchSubmitted(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should invoke onBatchStarted ") {

    val mockEvent = mock(classOf[StreamingListenerBatchStarted])
    this.listener.onBatchStarted(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should invoke onBatchCompleted ") {

    val mockEvent = mock(classOf[StreamingListenerBatchCompleted])
    this.listener.onBatchCompleted(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should invoke onOutputOperationStarted ") {

    val mockEvent = mock(classOf[StreamingListenerOutputOperationStarted])
    this.listener.onOutputOperationStarted(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("should invoke onOutputOperationCompleted ") {

    val mockEvent = mock(classOf[StreamingListenerOutputOperationCompleted])
    this.listener.onOutputOperationCompleted(mockEvent)
    verify(this.listener).logEvent(any(classOf[Option[JValue]]))

  }

  test("onStreamingStarted with  time  should populate expected TimeGenerated") {

    val mockEvent = SparkTestEvents.streamingListenerStreamingStartedEvent
    this.listener.onStreamingStarted(mockEvent)
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

  test("onReceiverStarted with no time field should populate TimeGeneratedField") {
    val mockEvent = SparkTestEvents.streamingListenerReceiverStartedEvent
    this.listener.onReceiverStarted(mockEvent)
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
