package org.apache.spark.listeners

import java.time.Instant

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}

trait UnifiedSparkListenerHandler extends SparkListener {
  protected def conf: SparkConf
  protected def logBlockUpdates: Boolean

  protected def logSparkListenerEvent(
                                     event: SparkListenerEvent,
                                     getTimestamp: () => Instant = () => Instant.now()
                                     ): Unit
}
