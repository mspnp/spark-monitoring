package org.apache.spark.listeners

import java.time.Instant

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
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

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = logSparkListenerEvent(event)

  override def onTaskStart(event: SparkListenerTaskStart): Unit = logSparkListenerEvent(event, () => {
    Instant.ofEpochMilli(event.taskInfo.launchTime)
  })

  override def onTaskGettingResult(event: SparkListenerTaskGettingResult): Unit = logSparkListenerEvent(event)

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = logSparkListenerEvent(event)

  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = {
    logSparkListenerEvent(redactEvent(event))
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    logSparkListenerEvent(event)
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = logSparkListenerEvent(event)

  override def onJobEnd(event: SparkListenerJobEnd): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.time)
  )

  override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded): Unit = {
    logSparkListenerEvent(event)
  }

  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved): Unit = {
    logSparkListenerEvent(event)
  }

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {
    logSparkListenerEvent(event)
  }

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    logSparkListenerEvent(event)
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    logSparkListenerEvent(event)
  }

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    logSparkListenerEvent(event)
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    logSparkListenerEvent(event)
  }

  override def onExecutorBlacklisted(event: SparkListenerExecutorBlacklisted): Unit = {
    logSparkListenerEvent(event)
  }

  override def onExecutorUnblacklisted(event: SparkListenerExecutorUnblacklisted): Unit = {
    logSparkListenerEvent(event)
  }

  override def onNodeBlacklisted(event: SparkListenerNodeBlacklisted): Unit = {
    logSparkListenerEvent(event)
  }

  override def onNodeUnblacklisted(event: SparkListenerNodeUnblacklisted): Unit = {
    logSparkListenerEvent(event)
  }

  override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {
    if (config.logBlockUpdates) {
      logSparkListenerEvent(event)
    }
  }

  // No-op because logging every update would be overkill
  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = {}

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    if (event.logEvent) {
      logSparkListenerEvent(event)
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

