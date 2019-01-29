package org.apache.spark.listeners

import java.time.Instant

import org.apache.spark.scheduler._
import org.apache.spark.util.Utils

trait SparkListenerHandlers {
  this: UnifiedSparkListenerHandler =>

  override val logBlockUpdates = conf.getBoolean(
    "spark.unifiedListener.logBlockUpdates",
    false
  )

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.time)
  )

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = logSparkListenerEvent(
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

  override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {
    if (this.logBlockUpdates) {
      logSparkListenerEvent(event)
    }
  }

  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit =
    logSparkListenerEvent(redactEvent(event))

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.time)
  )

  override def onExecutorBlacklisted(event: SparkListenerExecutorBlacklisted): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.time)
  )

  // No-op because logging every update would be overkill
  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = {}

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.time)
  )

  override def onExecutorUnblacklisted(event: SparkListenerExecutorUnblacklisted): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.time)
  )

  override def onJobEnd(event: SparkListenerJobEnd): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.time)
  )

  override def onJobStart(event: SparkListenerJobStart): Unit = logSparkListenerEvent(
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

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.stageInfo.completionTime.getOrElse(Instant.now().toEpochMilli))
  )

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.stageInfo.submissionTime.getOrElse(Instant.now().toEpochMilli))
  )

  override def onTaskGettingResult(event: SparkListenerTaskGettingResult): Unit = logSparkListenerEvent(event)

  override def onTaskStart(event: SparkListenerTaskStart): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.taskInfo.launchTime)
  )

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = logSparkListenerEvent(
    event,
    () => Instant.ofEpochMilli(event.taskInfo.finishTime)
  )

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {
    logSparkListenerEvent(event)
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
      name -> Utils.redact(this.conf, props)
    }
    SparkListenerEnvironmentUpdate(redactedProps)
  }
}
