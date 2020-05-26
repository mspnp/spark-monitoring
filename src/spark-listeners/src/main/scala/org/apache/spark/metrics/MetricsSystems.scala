package org.apache.spark.metrics

import com.codahale.metrics._
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import scala.collection.JavaConverters.mapAsScalaMapConverter

// These will only be created on executors
private[metrics] class RpcMetricsSystem(
                                         private val metricsSource: MetricsSource
                                       ) extends UserMetricsSystem with Logging {

  require(metricsSource != null, "metricsSource cannot be null")

  private val namespace = metricsSource.sourceName
  private val metricProxies = metricsSource.metricRegistry.getMetrics.asScala

  def counter(metricName: String): Counter = {
    getMetric[CounterProxy](metricName)
  }

  def histogram(metricName: String): Histogram = {
    getMetric[HistogramProxy](metricName)
  }

  def meter(metricName: String): Meter = {
    getMetric[MeterProxy](metricName)
  }

  def timer(metricName: String): Timer = {
    getMetric[TimerProxy](metricName)
  }

  def gauge[T](metricName: String): SettableGauge[T] = {
    getMetric[SettableGaugeProxy[T]](metricName)
  }

  private def getMetric[T <: MetricProxy](metricName: String): T = {
    metricProxies.get(metricName) match {
      case Some(metric) => {
        metric.asInstanceOf[T]
      }
      case None => throw new SparkException(s"Metric '${metricName}' in namespace ${namespace} was not found")
    }
  }
}

// These can be created on the driver and the executors.
class LocalMetricsSystem(
                          metricsSource: MetricsSource
                        ) extends UserMetricsSystem {

  require(metricsSource != null, "metricsSource cannot be null")

  private val namespace = metricsSource.sourceName
  private lazy val metrics = metricsSource.metricRegistry.getMetrics.asScala

  def counter(metricName: String): Counter = {
    getMetric[Counter](metricName)
  }

  def histogram(metricName: String): Histogram = {
    getMetric[Histogram](metricName)
  }

  def meter(metricName: String): Meter = {
    getMetric[Meter](metricName)
  }

  def timer(metricName: String): Timer = {
    getMetric[Timer](metricName)
  }

  def gauge[T](metricName: String): SettableGauge[T] = {
    val metric = getMetric[Gauge[T]](metricName)
    // If we have one, but it's not a settable gauge, it will run autonomously and provide metrics.
    // However, this is an exception here, as the developer wants to set it.
    if (!(metric.isInstanceOf[SettableGauge[T]])) {
      throw new SparkException(s"Gauge ${metricName} does not extend SettableGauge[T]")
    }

    metric.asInstanceOf[SettableGauge[T]]
  }

  private def getMetric[T <: Metric](metricName: String): T = {
    metrics.get(metricName) match {
      case Some(metric) => {
        metric.asInstanceOf[T]
      }
      case None => throw new SparkException(s"Metric '${metricName}' in namespace ${namespace} was not found")
    }
  }
}
