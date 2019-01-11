package org.apache.spark.listeners

import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId
import org.json4s.JsonAST.JValue
import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach


class LogAnalyticsListenerSuite extends ListenerSuite[LogAnalyticsListener]
  with BeforeAndAfterEach {

  test("should invoke onStageSubmitted ") {
    this.onSparkListenerEvent(this.listener.onStageSubmitted)
  }

  test("should invoke onTaskStart ") {
    this.onSparkListenerEvent(this.listener.onTaskStart)
  }

  test("should invoke onTaskGettingResult ") {
    this.onSparkListenerEvent(this.listener.onTaskGettingResult)
  }

  test("should invoke onTaskEnd ") {
    this.onSparkListenerEvent(this.listener.onTaskEnd)
  }

  test("should invoke onEnvironmentUpdate ") {
    val event = mock(classOf[SparkListenerEnvironmentUpdate])
    when(event.environmentDetails).thenReturn(Map("someKey" -> Seq(("tuple1-1", "tuple1-2"), ("tuple2-1", "tuple2-2"))))
    this.onSparkListenerEvent(this.listener.onEnvironmentUpdate, event)
  }

  test("should invoke onStageCompleted ") {
    this.onSparkListenerEvent(this.listener.onStageCompleted)
  }

  test("should invoke onJobStart ") {
    this.onSparkListenerEvent(this.listener.onJobStart)
  }

  test("should invoke onJobEnd ") {
    this.onSparkListenerEvent(this.listener.onJobEnd)
  }


  test("should invoke onBlockManagerAdded ") {
    this.onSparkListenerEvent(this.listener.onBlockManagerAdded)
  }

  test("should invoke onBlockManagerRemoved ") {
    this.onSparkListenerEvent(this.listener.onBlockManagerRemoved)
  }

  test("should invoke onUnpersistRDD ") {
    this.onSparkListenerEvent(this.listener.onUnpersistRDD)
  }

  test("should invoke onApplicationStart ") {
    this.onSparkListenerEvent(this.listener.onApplicationStart)
  }

  test("should invoke onApplicationEnd ") {
    this.onSparkListenerEvent(this.listener.onApplicationEnd)
  }

  test("should invoke onExecutorAdded ") {
    this.onSparkListenerEvent(this.listener.onExecutorAdded)
  }

  test("should invoke onExecutorRemoved ") {
    this.onSparkListenerEvent(this.listener.onExecutorRemoved)
  }

  test("should invoke onExecutorBlacklisted ") {
    this.onSparkListenerEvent(this.listener.onExecutorBlacklisted)
  }

  test("should invoke onNodeBlacklisted ") {
    this.onSparkListenerEvent(this.listener.onNodeBlacklisted)
  }

  test("should invoke onNodeUnblacklisted ") {
    this.onSparkListenerEvent(this.listener.onNodeUnblacklisted)
  }

  test("should not invoke onBlockUpdated when logBlockUpdates is set to false ") {
    val conf = new SparkConf()
    conf.set("spark.logAnalytics.workspaceId", "id")
    conf.set("spark.logAnalytics.secret", "secret")
    conf.set("spark.logAnalytics.logBlockUpdates", "false")
    this.listener = spy(new LogAnalyticsListener(conf))
    val event = mock(classOf[SparkListenerBlockUpdated])
    this.listener.onBlockUpdated(event)
    verify(this.listener, times(0)).logEvent(any(classOf[Option[JValue]]))
  }

  test("should invoke onBlockUpdated when logBlockUpdates is set to true ") {
    val conf = new SparkConf()
    conf.set("spark.logAnalytics.workspaceId", "id")
    conf.set("spark.logAnalytics.secret", "secret")
    conf.set("spark.logAnalytics.logBlockUpdates", "true")
    this.listener = spy(new LogAnalyticsListener(conf))
    this.onSparkListenerEvent(this.listener.onBlockUpdated)
  }

  test("should onExecutorMetricsUpdate be always no op") {
    val event = mock(classOf[SparkListenerExecutorMetricsUpdate])
    this.listener.onExecutorMetricsUpdate(event)
    verify(this.listener, times(0)).logEvent(any(classOf[Option[JValue]]))
  }

  test("should invoke onOtherEvent but don't log if logevent is not enabled ") {
    val mockEvent = mock(classOf[SparkListenerEvent])
    when(mockEvent.logEvent).thenReturn(false)
    this.listener.onOtherEvent(mockEvent)
    verify(this.listener, times(0)).logEvent(any(classOf[Option[JValue]]))
  }

  test("should  invoke onOtherEvent but will log logevent is  enabled ") {
    val event = mock(classOf[SparkListenerEvent])
    when(event.logEvent).thenReturn(true)
    this.onSparkListenerEvent(this.listener.onOtherEvent, event)
  }


  // these  test  tests the lamda function for following cases
  // scenario 1 - event has time stamp
  // scenario 2 - event has timestamp field but could be optional. in that case instant.now needs to plugged
  // scenario 3 - event has no explicit time stamp field. in that case, default lambda in the definition
  // should be picked up and should result in instant.now value
  test("should invoke onBlockManagerAdded with serialized mock Event with lamda  ") {
    val event = SparkListenerBlockManagerAdded(
      EPOCH_TIME,
      BlockManagerId.apply("driver", "localhost", 57967),
      278302556
    )
    this.assertTimeGenerated(
      this.onSparkListenerEvent(this.listener.onBlockManagerAdded, event),
      t => assert(t._2.extract[String] == EPOCH_TIME_AS_ISO8601)
    )
  }

  test("onStageSubmitted with submission time optional empty should populate TimeGenerated") {

    val event = SparkListenerStageSubmitted(
      new StageInfo(
        0,
        0,
        "dummy",
        1,
        Seq.empty,
        Seq.empty,
        "details")
    )

    this.assertTimeGenerated(
      this.onSparkListenerEvent(this.listener.onStageSubmitted, event),
      t => assert(!t._2.extract[String].isEmpty)
    )
  }

  test("onStageSubmitted with submission time should populate expected TimeGenerated") {
    val stageInfo = new StageInfo(
      0,
      0,
      "dummy",
      1,
      Seq.empty,
      Seq.empty,
      "details"
    )
    // submission time is not set via constructor
    stageInfo.submissionTime = Option(EPOCH_TIME)

    val event = SparkListenerStageSubmitted(
      stageInfo
    )

    this.assertTimeGenerated(
      this.onSparkListenerEvent(this.listener.onStageSubmitted, event),
      t => assert(t._2.extract[String] == EPOCH_TIME_AS_ISO8601)
    )
  }

  test("onEnvironmentUpdate should populate instant.now TimeGenerated field") {
    val event = SparkListenerEnvironmentUpdate(
      Map[String, Seq[(String, String)]](
        "JVM Information" -> Seq(("", "")),
        "Spark Properties" -> Seq(("", "")),
        "System Properties" -> Seq(("", "")),
        "Classpath Entries" -> Seq(("", "")))
    )

    this.assertTimeGenerated(
      this.onSparkListenerEvent(this.listener.onEnvironmentUpdate, event),
      t => assert(!t._2.extract[String].isEmpty)
    )
  }
}
