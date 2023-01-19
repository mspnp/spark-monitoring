package org.apache.spark.metrics.pnp

import com.codahale.metrics.MetricRegistry
import com.microsoft.pnp.loganalytics.LogAnalyticsSinkConfiguration
import org.apache.spark.SecurityManager
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.sink.Sink

import java.util.Properties

private class LogAnalyticsMetricsSink(
                                       val property: Properties,
                                       val registry: MetricRegistry,
                                       securityMgr: SecurityManager)
  extends Sink with Logging {

  private val config = new LogAnalyticsSinkConfiguration(property)
  var reporter = LogAnalyticsReporter.forRegistry(registry)
    .withWorkspaceId(config.workspaceId)
    .withWorkspaceKey(config.secret)
    .withLogType(config.logType)
    .build()

  org.apache.spark.metrics.MetricsSystem.checkMinimalPollingPeriod(config.pollUnit, config.pollPeriod)

  // This additional constructor allows the library to be used on Spark 3.2.x clusters
  // without the fix for https://issues.apache.org/jira/browse/SPARK-37078
  def this(
            property: Properties,
            registry: MetricRegistry) {
    this(property, registry, null)
  }

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
