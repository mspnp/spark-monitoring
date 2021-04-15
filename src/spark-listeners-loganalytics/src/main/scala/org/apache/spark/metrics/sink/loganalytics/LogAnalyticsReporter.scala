package org.apache.spark.metrics.sink.loganalytics

import java.time.Instant
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{Timer, _}
import com.codahale.metrics.json.MetricsModule
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.microsoft.pnp.SparkInformation
import com.microsoft.pnp.client.loganalytics.{LogAnalyticsClient, LogAnalyticsSendBufferClient}
import org.apache.spark.internal.Logging
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import scala.util.control.NonFatal

object LogAnalyticsReporter {
  /**
    * Returns a new {@link Builder} for {@link LogAnalyticsReporter}.
    *
    * @param registry the registry to report
    * @return a { @link Builder} instance for a { @link LogAnalyticsReporter}
    */
  def forRegistry(registry: MetricRegistry) = new LogAnalyticsReporter.Builder(registry)

  /**
    * A builder for {@link LogAnalyticsReporter} instances. Defaults to not using a prefix, using the default clock, converting rates to
    * events/second, converting durations to milliseconds, and not filtering metrics. The default
    * Log Analytics log type is DropWizard
    */
  class Builder(val registry: MetricRegistry) extends Logging {
    private var clock = Clock.defaultClock
    private var prefix: String = null
    private var rateUnit = TimeUnit.SECONDS
    private var durationUnit = TimeUnit.MILLISECONDS
    private var filter = MetricFilter.ALL
    private var filterRegex = sys.env.getOrElse("LA_SPARKMETRIC_REGEX", "")
    if(filterRegex != "") {
      filter = new MetricFilter() {
        override def matches(name: String, metric: Metric): Boolean = {
          name.matches(filterRegex)
        }
      }
    }
    private var logType = "SparkMetrics"
    private var workspaceId: String = null
    private var workspaceKey: String = null

    /**
      * Use the given {@link Clock} instance for the time. Usually the default clock is sufficient.
      *
      * @param clock clock
      * @return { @code this}
      */
    def withClock(clock: Clock): LogAnalyticsReporter.Builder = {
      this.clock = clock
      this
    }

    /**
      * Configure a prefix for each metric name. Optional, but useful to identify originator of metric.
      *
      * @param prefix prefix for metric name
      * @return { @code this}
      */
    def prefixedWith(prefix: String): LogAnalyticsReporter.Builder = {
      this.prefix = prefix
      this
    }

    /**
      * Convert all the rates to a certain TimeUnit, defaults to TimeUnit.SECONDS.
      *
      * @param rateUnit unit of rate
      * @return { @code this}
      */
    def convertRatesTo(rateUnit: TimeUnit): LogAnalyticsReporter.Builder = {
      this.rateUnit = rateUnit
      this
    }

    /**
      * Convert all the durations to a certain TimeUnit, defaults to TimeUnit.MILLISECONDS
      *
      * @param durationUnit unit of duration
      * @return { @code this}
      */
    def convertDurationsTo(durationUnit: TimeUnit): LogAnalyticsReporter.Builder = {
      this.durationUnit = durationUnit
      this
    }

    /**
      * Allows to configure a special MetricFilter, which defines what metrics are reported
      *
      * @param filter metrics filter
      * @return { @code this}
      */
    def filter(filter: MetricFilter): LogAnalyticsReporter.Builder = {
      this.filter = filter
      this
    }

    /**
      * The log type to send to Log Analytics. Defaults to 'SparkMetrics'.
      *
      * @param logType Log Analytics log type
      * @return { @code this}
      */
    def withLogType(logType: String): LogAnalyticsReporter.Builder = {
      logInfo(s"Setting logType to '${logType}'")
      this.logType = logType
      this
    }

    /**
      * The workspace id of the Log Analytics workspace
      *
      * @param workspaceId Log Analytics workspace id
      * @return { @code this}
      */
    def withWorkspaceId(workspaceId: String): LogAnalyticsReporter.Builder = {
      logInfo(s"Setting workspaceId to '${workspaceId}'")
      this.workspaceId = workspaceId
      this
    }

    /**
      * The workspace key of the Log Analytics workspace
      *
      * @param workspaceKey Log Analytics workspace key
      * @return { @code this}
      */
    def withWorkspaceKey(workspaceKey: String): LogAnalyticsReporter.Builder = {
      this.workspaceKey = workspaceKey
      this
    }

    /**
      * Builds a {@link LogAnalyticsReporter} with the given properties.
      *
      * @return a { @link LogAnalyticsReporter}
      */
    def build(): LogAnalyticsReporter = {
      logDebug("Creating LogAnalyticsReporter")
      new LogAnalyticsReporter(
        registry,
        workspaceId,
        workspaceKey,
        logType,
        clock,
        prefix,
        rateUnit,
        durationUnit,
        filter
      )
    }
  }
}

class LogAnalyticsReporter(val registry: MetricRegistry, val workspaceId: String, val workspaceKey: String, val logType: String, val clock: Clock, val prefix: String, val rateUnit: TimeUnit, val durationUnit: TimeUnit, val filter: MetricFilter)//, var additionalFields: util.Map[String, AnyRef]) //this.logType);
  extends ScheduledReporter(registry, "loganalytics-reporter", filter, rateUnit, durationUnit)
    with Logging {
  private val mapper = new ObjectMapper()
    .registerModules(
      DefaultScalaModule,
      new MetricsModule(
        rateUnit,
        durationUnit,
        true,
        filter
      )
    )

  private val logAnalyticsBufferedClient = new LogAnalyticsSendBufferClient(
    new LogAnalyticsClient(this.workspaceId, this.workspaceKey),
    "SparkMetric"
  )


  override def report(
                       gauges: java.util.SortedMap[String, Gauge[_]],
                       counters: java.util.SortedMap[String, Counter],
                       histograms: java.util.SortedMap[String, Histogram],
                       meters: java.util.SortedMap[String, Meter],
                       timers: java.util.SortedMap[String, Timer]): Unit = {
    logDebug("Reporting metrics")
    // nothing to do if we don't have any metrics to report
    if (gauges.isEmpty && counters.isEmpty && histograms.isEmpty && meters.isEmpty && timers.isEmpty) {
      logInfo("All metrics empty, nothing to report")
      return
    }
    val now = Instant.now
    import scala.collection.JavaConversions._

    val ambientProperties = SparkInformation.get() + ("SparkEventTime" -> now.toString)
    val metrics = gauges.retain((_, v) => v.getValue != null).toSeq ++
      counters.toSeq ++ histograms.toSeq ++ meters.toSeq ++ timers.toSeq
    for ((name, metric) <- metrics) {
      try {
        this.logAnalyticsBufferedClient.sendMessage(
          compact(this.addProperties(name, metric, ambientProperties)),
          "SparkMetricTime"
        )
      } catch {
        case NonFatal(e) =>
          logError(s"Error serializing metric to JSON", e)
          None
      }
    }
  }

  //private def addProperties(name: String, metric: Metric, timestamp: Instant): JValue = {
  private def addProperties(name: String, metric: Metric, properties: Map[String, String]): JValue = {
    val metricType: String = metric match {
      case _: Counter => classOf[Counter].getSimpleName
      case _: Gauge[_] => classOf[Gauge[_]].getSimpleName
      case _: Histogram => classOf[Histogram].getSimpleName
      case _: Meter => classOf[Meter].getSimpleName
      case _: Timer => classOf[Timer].getSimpleName
      case m: Metric => m.getClass.getSimpleName
    }

    parse(this.mapper.writeValueAsString(metric))
      .merge(render(
        ("metric_type" -> metricType) ~
          ("name" -> name) ~
          properties
      ))
  }
}
