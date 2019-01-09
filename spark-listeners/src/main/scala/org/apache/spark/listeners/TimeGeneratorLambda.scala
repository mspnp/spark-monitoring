package org.apache.spark.listeners

import org.apache.spark.scheduler.{SparkListenerJobEnd, SparkListenerJobStart, SparkListenerTaskStart}

object TimeGeneratorLambda {


  val defaultLambda: AnyRef => String = (o: AnyRef) => {
    LogAnalyticsTimeGenerator.getTime()
  }

  val onTaskStartLambda: AnyRef => String = (o: AnyRef) => {
    val event = o.asInstanceOf[SparkListenerTaskStart]
    LogAnalyticsTimeGenerator.getTime(event.taskInfo.launchTime)
  }

  val onSparkListenerJobEndLambda: AnyRef => String = (o: AnyRef) => {
    val event = o.asInstanceOf[SparkListenerJobEnd]
    LogAnalyticsTimeGenerator.getTime(event.time)
  }
  val onSparkListenerJobStartLambda: AnyRef => String = (o: AnyRef) => {
    val event = o.asInstanceOf[SparkListenerJobStart]
    LogAnalyticsTimeGenerator.getTime(event.time)
  }
}
