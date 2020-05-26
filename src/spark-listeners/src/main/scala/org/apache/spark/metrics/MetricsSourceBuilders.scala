package org.apache.spark.metrics

import com.codahale.metrics._
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.Implicits.StringExtensions
import org.apache.spark.util.RpcUtils
import org.apache.spark.{SparkContext, SparkEnv, SparkException}

abstract class MetricsSourceBuilder(protected val namespace: String) extends Logging {

  require(!namespace.isNullOrEmpty, "namespace cannot be null, empty, or only whitespace")

  protected val metricRegistry = new MetricRegistry

  def registerCounter(name: String): this.type

  def registerHistogram(name: String): this.type

  def registerMeter(name: String): this.type

  def registerTimer(name: String): this.type
}

class LocalMetricsSourceBuilder(override val namespace: String)
  extends MetricsSourceBuilder(namespace) {

  override def registerCounter(name: String): this.type = {
    register(name, new Counter)
  }

  override def registerHistogram(name: String): this.type = {
    register(name, new Histogram(new ExponentiallyDecayingReservoir))
  }

  override def registerMeter(name: String): this.type = {
    register(name, new Meter)
  }

  override def registerTimer(name: String): this.type = {
    register(name, new Timer)
  }

  def register[T <: Metric](name: String, metric: T)(implicit ev: T <:!< MetricProxy): this.type = {
    require(!name.isNullOrEmpty, "name cannot be null, empty, or only whitespace")
    this.metricRegistry.register[T](MetricRegistry.name(name), metric)
    this
  }

  private[metrics] def build(): MetricsSource = {
    MetricsSource(this.namespace, this.metricRegistry)
  }
}

class RemoteMetricsSourceBuilder(override val namespace: String,
                                 val endpointName: String,
                                 val sparkEnv: SparkEnv)
  extends MetricsSourceBuilder(namespace) {

  if (sparkEnv.executorId == SparkContext.DRIVER_IDENTIFIER) {
    throw new IllegalStateException("RemoteMetricsSourceBuilder cannot be used on a driver")
  }

  val endpointRef = try {
    RpcUtils.makeDriverRef(endpointName, sparkEnv.conf, sparkEnv.rpcEnv)
  } catch {
    case e: SparkException => {
      logError("Could not create RPC driver reference", e)
      throw e
    }
  }

  override def registerCounter(name: String): this.type = {
    register(name, new CounterProxy(
      this.endpointRef,
      this.namespace,
      name
    ))
  }

  override def registerHistogram(name: String): this.type = {
    register(name, new HistogramProxy(
      this.endpointRef,
      this.namespace,
      name
    ))
  }

  override def registerMeter(name: String): this.type = {
    register(name, new MeterProxy(
      this.endpointRef,
      this.namespace,
      name
    ))
  }

  override def registerTimer(name: String): this.type = {
    register(name, new TimerProxy(
      this.endpointRef,
      this.namespace,
      name
    ))
  }

  def register[T <: MetricProxy](name: String, metric: T): this.type = {
    require(!name.isNullOrEmpty, "name cannot be null, empty, or only whitespace")
    this.metricRegistry.register[T](MetricRegistry.name(name), metric)
    this
  }

  private[metrics] def build(): MetricsSource = {
    MetricsSource(this.namespace, this.metricRegistry)
  }
}
