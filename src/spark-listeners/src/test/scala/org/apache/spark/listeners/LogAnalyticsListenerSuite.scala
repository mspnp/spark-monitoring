package org.apache.spark.listeners

import java.util.Properties

import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.storage.{BlockManagerId, BlockUpdatedInfo, RDDBlockId, StorageLevel}
import org.apache.spark.util.{AccumulatorMetadata, LongAccumulator}
import org.apache.spark.{SparkConf, Success, TaskEndReason, TaskState}
import org.json4s.JsonAST.JValue
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterEach, PrivateMethodTester}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.metrics.TestUtils._

case class TestOtherEvent(
                           val myInt: Int,
                           override val logEvent: Boolean = true
                         ) extends SparkListenerEvent

object LogAnalyticsListenerSuite {
  val sparkListenerJobStart = SparkListenerJobStart(
    jobId = 0,
    time = System.currentTimeMillis(),
    stageInfos = Seq(
      createStageInfo(0, 0),
      createStageInfo(1, 0)
    ),
    createProperties(0))

  val sparkListenerJobEnd = SparkListenerJobEnd(
    jobId = 0,
    time = System.currentTimeMillis(),
    JobSucceeded)

  val sparkListenerTaskStart = SparkListenerTaskStart(
    0,
    0,
    createTaskInfo(0, 0))

  val sparkListenerTaskGettingResult = SparkListenerTaskGettingResult(
    createTaskInfo(0, 0)
  )

  //Spark 3 requires an additional parameter. This is the best way to not duplicate code and support both.
   val sparkListenerTaskEnd = {
     import org.apache.spark.metrics.TestImplicits._
     val spark2args= List[Any](0,0,"",Success.asInstanceOf[TaskEndReason],LogAnalyticsListenerSuite.createTaskInfo(0, 0),new TaskMetrics())

     val spark3args = spark2args.insertAt(5, loadOneOf("org.apache.spark.executor.ExecutorMetrics").map(_.newInstance()).orNull)

     newInstance(classOf[SparkListenerTaskEnd], spark2args:_*)
         .orElse(newInstance(classOf[SparkListenerTaskEnd], spark3args:_*))
         .get
   }


  val sparkListenerStageSubmitted = SparkListenerStageSubmitted(createStageInfo(0, 0))
  sparkListenerStageSubmitted.stageInfo.submissionTime = Option(ListenerSuite.EPOCH_TIME)

  val sparkListenerStageSubmittedNoneSubmissionTime = SparkListenerStageSubmitted(createStageInfo(0, 0))

  val sparkListenerStageCompleted = SparkListenerStageCompleted(createStageInfo(0, 0))

  val sparkListenerBlockManagerAdded = SparkListenerBlockManagerAdded(
    1L,
    BlockManagerId("1", "1.example.com", 42),
    42L
  )

  val sparkListenerBlockManagerRemoved = SparkListenerBlockManagerRemoved(
    1L,
    BlockManagerId("1", "1.example.com", 42)
  )

  val sparkListenerUnpersistRDD = SparkListenerUnpersistRDD(42)

  val sparkListenerApplicationStart = SparkListenerApplicationStart(
    "name",
    Some("id"),
    1L,
    "user",
    Some("attempt"),
    None)

  val sparkListenerApplicationEnd = SparkListenerApplicationEnd(1L)

  val sparkListenerExecutorAdded = SparkListenerExecutorAdded(1L, "1",
    new ExecutorInfo(s"1.example.com", 1, Map()))

  val sparkListenerExecutorRemoved = SparkListenerExecutorRemoved(1L, "1", "Test")

  val sparkListenerExecutorBlacklisted = SparkListenerExecutorBlacklisted(1L, "1", 42)

  val sparkListenerExecutorUnblacklisted = SparkListenerExecutorUnblacklisted(1L, "1")

  val sparkListenerNodeBlacklisted = SparkListenerNodeBlacklisted(1L, "1.example.com", 2)

  val sparkListenerNodeUnblacklisted = SparkListenerNodeUnblacklisted(1L, "1.example.com")

  val sparkListenerEnvironmentUpdate = SparkListenerEnvironmentUpdate(
    Map(
      "JVM Information" -> Seq(
        "Java Version" -> sys.props("java.version"),
        "Java Home" -> sys.props("java.home"),
        "Scala Version" -> scala.util.Properties.versionString
      ),
      "Spark Properties" -> Seq(
        "spark.conf.1" -> "1",
        "spark.conf.2" -> "2"
      ),
      "System Properties" -> Seq(
        "sys.prop.1" -> "1",
        "sys.prop.2" -> "2"
      ),
      "Classpath Entries" -> Seq(
        "/jar1" -> "System",
        "/jar2" -> "User"
      ),
      "Hadoop Properties" -> Seq()
    )
  )

  val sparkListenerBlockUpdated = SparkListenerBlockUpdated(
    BlockUpdatedInfo(
      BlockManagerId("1", "1.example.com", 42),
      RDDBlockId(1, 1),
      StorageLevel.MEMORY_AND_DISK,
      1L,
      2L))

  private def createTaskInfo(
                              taskId: Int,
                              attemptNumber: Int,
                              accums: Map[Long, Long] = Map.empty): TaskInfo = {
    val info = new TaskInfo(
      taskId = taskId,
      attemptNumber = attemptNumber,
      // The following fields are not used in tests
      index = 0,
      launchTime = 0,
      executorId = "",
      host = "",
      taskLocality = TaskLocality.PROCESS_LOCAL,
      speculative = false)
    info.markFinished(TaskState.FINISHED, 1L)
    info.setAccumulables(createAccumulatorInfos(accums))
    info
  }

  private def createAccumulatorInfos(accumulatorUpdates: Map[Long, Long]): Seq[AccumulableInfo] = {
    accumulatorUpdates.map { case (id, value) =>
      val acc = new LongAccumulator
      acc.metadata = AccumulatorMetadata(id, None, false)
      acc.toInfo(Some(value), None)
    }.toSeq
  }

  private def createStageInfo(stageId: Int, attemptId: Int): StageInfo = {
    // Note - this workaround will no longer be needed once versions before 3.1.1 are no longer supported
    // StageInfo 2.4.5-: (stageId: Int, attemptId: Int, name: String, numTasks: Int, rddInfos: Seq[org.apache.spark.storage.RDDInfo], parentIds: Seq[Int], details: String, taskMetrics: org.apache.spark.executor.TaskMetrics, taskLocalityPreferences: Seq[Seq[org.apache.spark.scheduler.TaskLocation]])
    // StageInfo 3.0.1-: (stageId: Int, attemptId: Int, name: String, numTasks: Int, rddInfos: Seq[org.apache.spark.storage.RDDInfo], parentIds: Seq[Int], details: String, taskMetrics: org.apache.spark.executor.TaskMetrics, taskLocalityPreferences: Seq[Seq[org.apache.spark.scheduler.TaskLocation]], shuffleDepId: Option[Int])
    // StageInfo 3.1.1+: (stageId: Int, attemptId: Int, name: String, numTasks: Int, rddInfos: Seq[org.apache.spark.storage.RDDInfo], parentIds: Seq[Int], details: String, taskMetrics: org.apache.spark.executor.TaskMetrics, taskLocalityPreferences: Seq[Seq[org.apache.spark.scheduler.TaskLocation]], shuffleDepId: Option[Int], resourceProfileId: Int)

    val v24args = List[Any](stageId,attemptId,"",0,Nil,Nil,"",new TaskMetrics(),Nil)
    val v30args = List[Any](stageId,attemptId,"",0,Nil,Nil,"",new TaskMetrics(),Nil,Option(0))
    val v31args = List[Any](stageId,attemptId,"",0,Nil,Nil,"",new TaskMetrics(),Nil,Option(0),0)

    newInstance(classOf[StageInfo], v31args:_*)
         .orElse(newInstance(classOf[StageInfo], v30args:_*))
         .orElse(newInstance(classOf[StageInfo], v24args:_*))
         .get
  }

  private def createProperties(executionId: Long): Properties = {
    val properties = new Properties()
    properties.setProperty("executionId", executionId.toString)
    properties
  }
}

class LogAnalyticsListenerSuite extends ListenerSuite
  with BeforeAndAfterEach
  with PrivateMethodTester {

  test("should invoke onStageSubmitted ") {
    this.onSparkListenerEvent(
      this.listener.onStageSubmitted,
      LogAnalyticsListenerSuite.sparkListenerStageSubmitted
    )
  }

  test("should invoke onTaskStart ") {
    this.onSparkListenerEvent(
      this.listener.onTaskStart,
      LogAnalyticsListenerSuite.sparkListenerTaskStart
    )
  }

  test("should invoke onTaskGettingResult ") {
    this.onSparkListenerEvent(
      this.listener.onTaskGettingResult,
      LogAnalyticsListenerSuite.sparkListenerTaskGettingResult
    )
  }

  test("should invoke onTaskEnd ") {
    this.onSparkListenerEvent(
      this.listener.onTaskEnd,
      LogAnalyticsListenerSuite.sparkListenerTaskEnd
    )
  }

  test("should invoke onEnvironmentUpdate ") {
    this.onSparkListenerEvent(
      this.listener.onEnvironmentUpdate,
      LogAnalyticsListenerSuite.sparkListenerEnvironmentUpdate
    )
  }

  test("should invoke onStageCompleted ") {
    this.onSparkListenerEvent(
      this.listener.onStageCompleted,
      LogAnalyticsListenerSuite.sparkListenerStageCompleted
    )
  }

  test("should invoke onJobStart ") {
    this.onSparkListenerEvent(
      this.listener.onJobStart,
      LogAnalyticsListenerSuite.sparkListenerJobStart
    )
  }

  test("should invoke onJobEnd ") {
    this.onSparkListenerEvent(
      this.listener.onJobEnd,
      LogAnalyticsListenerSuite.sparkListenerJobEnd
    )
  }

  test("should invoke onBlockManagerAdded ") {
    this.onSparkListenerEvent(
      this.listener.onBlockManagerAdded,
      LogAnalyticsListenerSuite.sparkListenerBlockManagerAdded
    )
  }

  test("should invoke onBlockManagerRemoved ") {
    this.onSparkListenerEvent(
      this.listener.onBlockManagerRemoved,
      LogAnalyticsListenerSuite.sparkListenerBlockManagerRemoved
    )
  }

  test("should invoke onUnpersistRDD ") {
    this.onSparkListenerEvent(
      this.listener.onUnpersistRDD,
      LogAnalyticsListenerSuite.sparkListenerUnpersistRDD
    )
  }

  test("should invoke onApplicationStart ") {
    this.onSparkListenerEvent(
      this.listener.onApplicationStart,
      LogAnalyticsListenerSuite.sparkListenerApplicationStart
    )
  }

  test("should invoke onApplicationEnd ") {
    this.onSparkListenerEvent(
      this.listener.onApplicationEnd,
      LogAnalyticsListenerSuite.sparkListenerApplicationEnd
    )
  }

  test("should invoke onExecutorAdded ") {
    this.onSparkListenerEvent(
      this.listener.onExecutorAdded,
      LogAnalyticsListenerSuite.sparkListenerExecutorAdded
    )
  }

  test("should invoke onExecutorRemoved ") {
    this.onSparkListenerEvent(
      this.listener.onExecutorRemoved,
      LogAnalyticsListenerSuite.sparkListenerExecutorRemoved
    )
  }

  test("should invoke onExecutorBlacklisted ") {
    this.onSparkListenerEvent(
      this.listener.onExecutorBlacklisted,
      LogAnalyticsListenerSuite.sparkListenerExecutorBlacklisted
    )
  }

  test("should invoke onExecutorUnblacklisted ") {
    this.onSparkListenerEvent(
      this.listener.onExecutorUnblacklisted,
      LogAnalyticsListenerSuite.sparkListenerExecutorUnblacklisted
    )
  }

  test("should onExecutorMetricsUpdate be always no op") {
    val event = mock(classOf[SparkListenerExecutorMetricsUpdate])
    this.listener.onExecutorMetricsUpdate(event)
    verify(this.listener, times(0)).sendToSink(any(classOf[Option[JValue]]))
  }

  test("should invoke onNodeBlacklisted ") {
    this.onSparkListenerEvent(
      this.listener.onNodeBlacklisted,
      LogAnalyticsListenerSuite.sparkListenerNodeBlacklisted
    )
  }

  test("should invoke onNodeUnblacklisted ") {
    this.onSparkListenerEvent(
      this.listener.onNodeUnblacklisted,
      LogAnalyticsListenerSuite.sparkListenerNodeUnblacklisted
    )
  }

  test("should not invoke onBlockUpdated when logBlockUpdates is set to false ") {
    val conf = new SparkConf()
        .set("spark.driver.allowMultipleContexts", "true")
        .set("spark.unifiedListener.sink", classOf[TestSparkListenerSink].getName)
        .set("spark.unifiedListener.logBlockUpdates", "false")
    this.listener = spy(new UnifiedSparkListener(conf))
    this.listener.onBlockUpdated(LogAnalyticsListenerSuite.sparkListenerBlockUpdated)
    verify(this.listener, times(0)).sendToSink(any(classOf[Option[JValue]]))
  }

  test("should invoke onBlockUpdated when logBlockUpdates is set to true ") {
    val conf = new SparkConf()
        .set("spark.driver.allowMultipleContexts", "true")
        .set("spark.unifiedListener.sink", classOf[TestSparkListenerSink].getName)
        .set("spark.unifiedListener.logBlockUpdates", "true")
    this.listener = spy(new UnifiedSparkListener(conf))
    this.onSparkListenerEvent(
      this.listener.onBlockUpdated,
      LogAnalyticsListenerSuite.sparkListenerBlockUpdated
    )
  }

  test("should invoke onOtherEvent but don't log if logevent is not enabled ") {
    val event = TestOtherEvent(42, false)
    this.listener.onOtherEvent(event)
    verify(this.listener, times(0)).sendToSink(any(classOf[Option[JValue]]))
  }

  test("should invoke onOtherEvent but will log logevent is enabled ") {
    val event = TestOtherEvent(42)
    this.onSparkListenerEvent(this.listener.onOtherEvent, event)
  }


  // these test the lambda function for following cases
  // scenario 1 - event has timestamp
  // scenario 2 - event has timestamp field but could be optional.  The handling method should pass in
  // a lambda
  // scenario 3 - event has no explicit timestamp field. In this case, the default lambda will be used,
  // which uses Instant.now.
  test("onStageSubmitted with time should populate expected SparkEventTime") {
    val (json, _) = this.onSparkListenerEvent(
      this.listener.onBlockManagerAdded,
      SparkListenerBlockManagerAdded(
        ListenerSuite.EPOCH_TIME,
        BlockManagerId.apply("driver", "localhost", 57967),
        278302556
      )
    )
    this.assertSparkEventTime(
      json,
      (_, value) => assert(value.extract[String] === ListenerSuite.EPOCH_TIME_AS_ISO8601)
    )
  }

  test("onStageSubmitted with no submission time should populate SparkEventTime") {
    val (json, _) = this.onSparkListenerEvent(
      this.listener.onStageSubmitted,
      LogAnalyticsListenerSuite.sparkListenerStageSubmittedNoneSubmissionTime
    )

    this.assertSparkEventTime(
      json,
      (_, value) => assert(!value.extract[String].isEmpty)
    )
  }

  test("onStageSubmitted with submission time should populate expected SparkEventTime") {
    val (json, _) = this.onSparkListenerEvent(
      this.listener.onStageSubmitted,
      LogAnalyticsListenerSuite.sparkListenerStageSubmitted
    )

    this.assertSparkEventTime(
      json,
      (_, value) => assert(value.extract[String] === ListenerSuite.EPOCH_TIME_AS_ISO8601)
    )
  }

  test("onEnvironmentUpdate should populate SparkEventTime field") {
    val (json, _) = this.onSparkListenerEvent(
      this.listener.onEnvironmentUpdate,
      LogAnalyticsListenerSuite.sparkListenerEnvironmentUpdate
    )

    this.assertSparkEventTime(
      json,
      (_, value) => assert(!value.extract[String].isEmpty)
    )
  }

  test("createSink should be called") {
    this.listener.onJobStart(LogAnalyticsListenerSuite.sparkListenerJobStart)
    Thread.sleep(5000);
    //doNothing.when(this.listener).sendToSink(any(classOf[Option[JValue]]))
  }
}
