package org.apache.spark.listeners

import java.time.Instant

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.util.Utils
import org.apache.spark.{LogAnalytics, LogAnalyticsListenerConfiguration, SparkConf}

/**
  * A SparkListener that logs events to a Log Analytics workspace.
  *
  * Event logging is specified by the following configurable parameters:
  *   spark.logAnalytics.workspaceId - Log Analytics Workspace ID
  *   spark.logAnalytics.workspaceKey" - Key for the Log Analytics Workspace ID
  *   spark.logAnalytics.logType" - Optional Log Type name for Log Analytics
  *   spark.logAnalytics.timestampFieldName" - Optional field name for the event timestamp
  *   spark.logAnalytics.logBlockUpdates" - Optional setting specifying whether or not to log block updates
  */
class LogAnalyticsListener(sparkConf: SparkConf)
  extends SparkListener with Logging with LogAnalytics {

  val config = new LogAnalyticsListenerConfiguration(sparkConf)

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.stageInfo.submissionTime.getOrElse(Instant.now().toEpochMilli))
  )

  override def onTaskStart(event: SparkListenerTaskStart): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.taskInfo.launchTime)
  )

  override def onTaskGettingResult(event: SparkListenerTaskGettingResult): Unit = logSparkListenerEvent(event)

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.taskInfo.finishTime)
  )

  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit =
    logSparkListenerEvent(redactEvent(event))


  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.stageInfo.completionTime.getOrElse(Instant.now().toEpochMilli))
  )


  override def onJobStart(event: SparkListenerJobStart): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.time)
  )

  override def onJobEnd(event: SparkListenerJobEnd): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.time)
  )

  override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.time)
  )


  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.time)
  )


  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {
    logSparkListenerEvent(event)
  }

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.time)
  )


  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.time)
  )

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.time)
  )

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.time)
  )

  override def onExecutorBlacklisted(event: SparkListenerExecutorBlacklisted): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.time)
  )

  override def onExecutorUnblacklisted(event: SparkListenerExecutorUnblacklisted): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.time)
  )

  override def onNodeBlacklisted(event: SparkListenerNodeBlacklisted): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.time)
  )

  override def onNodeUnblacklisted(event: SparkListenerNodeUnblacklisted): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.time)
  )

  override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {
    if (config.logBlockUpdates) {
      logSparkListenerEvent(event)
    }
  }

  // No-op because logging every update would be overkill
  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = {}

  //Streaming query events extends spark listener events.We do have
  //custom streaming query listeners implemented. in that case would
  //not want to log them through this method.

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case _: StreamingQueryListener.Event =>
      case sparkListenerEvent: SparkListenerEvent => if (sparkListenerEvent.logEvent) {
        logSparkListenerEvent(sparkListenerEvent)
      }

    }
  }

  private def redactEvent(event: SparkListenerEnvironmentUpdate): SparkListenerEnvironmentUpdate = {
    // environmentDetails maps a string descriptor to a set of properties
    // Similar to:
    // "JVM Information" -> jvmInformation,
    // "Spark Properties" -> sparkProperties,
    // ...
    // where jvmInformation, sparkProperties, etc. are sequence of tuples.
    // We go through the various  of properties and redact sensitive information from them.
    val redactedProps = event.environmentDetails.map { case (name, props) =>
      name -> Utils.redact(sparkConf, props)
    }
    SparkListenerEnvironmentUpdate(redactedProps)
  }
}

