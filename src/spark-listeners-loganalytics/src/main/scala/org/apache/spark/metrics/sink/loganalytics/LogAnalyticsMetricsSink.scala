package org.apache.spark.metrics.sink.loganalytics

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.sink.Sink
import org.apache.spark.{SecurityManager, SparkException}

private class LogAnalyticsMetricsSink(
                                val property: Properties,
                                val registry: MetricRegistry,
                                securityMgr: SecurityManager)
  extends Sink with Logging {

  // This additional constructor allows the library to be used on Spark 3.2.x clusters
  // without the fix for https://issues.apache.org/jira/browse/SPARK-37078
  def this(
        property: Properties,
        registry: MetricRegistry)
  {
    this(property, registry, null)
  }

  private val config = new LogAnalyticsSinkConfiguration(property)

  org.apache.spark.metrics.MetricsSystem.checkMinimalPollingPeriod(config.pollUnit, config.pollPeriod)

  var reporter = LogAnalyticsReporter.forRegistry(registry)
    .withWorkspaceId(config.workspaceId)
    .withWorkspaceKey(config.secret)
    .withLogType(config.logType)
    .build()

  override def start(): Unit = {
    reporter.start(config.pollPeriod, config.pollUnit)
    logInfo(s"LogAnalyticsMetricsSink started")
  }

  override def stop(): Unit = {
    reporter.stop()
    logInfo("LogAnalyticsMetricsSink stopped.")
  }

  override def report(): Unit = {
    reporter.report()
  }
}
