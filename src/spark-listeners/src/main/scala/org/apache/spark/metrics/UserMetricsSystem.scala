package org.apache.spark.metrics

import java.util.concurrent.ConcurrentHashMap

import com.codahale.metrics._
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.Implicits.StringExtensions
import org.apache.spark.{SparkContext, SparkEnv}

import scala.collection.JavaConverters.mapAsScalaConcurrentMapConverter
import scala.collection.mutable

trait UserMetricsSystem {
  def counter(metricName: String): Counter

  def histogram(metricName: String): Histogram

  def meter(metricName: String): Meter

  def timer(metricName: String): Timer

  def gauge[T](metricName: String): SettableGauge[T]
}

object UserMetricsSystems extends Logging {
  // This is for registries local to our current environment (i.e. driver or executor)
  @transient private lazy val metricsSystems =
    new ConcurrentHashMap[String, UserMetricsSystem].asScala

  // This method is only for "local" (i.e. driver OR executor) metrics.
  // These systems can be queried and used by name.
  def getMetricSystem(namespace: String, create: (LocalMetricsSourceBuilder) => Unit): LocalMetricsSystem = {
    metricsSystems.getOrElseUpdate(namespace, {
      logInfo(s"Creating LocalMetricsSystem ${namespace}")
      val builder = new LocalMetricsSourceBuilder(namespace)
      create(builder)
      val metricsSource = builder.build
      // Register here for now!
      SparkEnv.get.metricsSystem.registerSource(metricsSource)
      new LocalMetricsSystem(metricsSource)
    }).asInstanceOf[LocalMetricsSystem]
  }

  def buildReceiverMetricSystem(
                                 sparkEnv: SparkEnv,
                                 create: (ReceiverMetricSystemBuilder) => Unit,
                                 endpointName: String = RpcMetricsReceiver.DefaultEndpointName
                               ): Unit = {
    // Just to be safe, we will throw an exception if the user tries to run this on the driver
    if (sparkEnv.executorId != SparkContext.DRIVER_IDENTIFIER) {
      // We are on the driver, so this is invalid
      throw new IllegalStateException(s"buildReceiverMetricSystem cannot be invoked on a Spark executor")
    }
    val builder = new ReceiverMetricSystemBuilder(sparkEnv, endpointName)
    create(builder)
    builder.build
  }

  // This should only be used on the executors.
  // They can be queried by name as well.
  def getRemoteMetricSystem(
                       namespace: String,
                       create: RemoteMetricsSourceBuilder => Unit,
                       endpointName: String = RpcMetricsReceiver.DefaultEndpointName): RpcMetricsSystem = {
    // Just to be safe, we will throw an exception if the user tries to run this on the driver
    if (SparkEnv.get.executorId == SparkContext.DRIVER_IDENTIFIER) {
      // We are on the driver, so this is invalid
      throw new IllegalStateException(s"getRemoteMetricSystem cannot be invoked on a Spark driver")
    }
    metricsSystems.getOrElseUpdate(
      namespace, {
        val builder = new RemoteMetricsSourceBuilder(namespace, endpointName, SparkEnv.get)
        create(builder)
        new RpcMetricsSystem(builder.build)
      }
    ).asInstanceOf[RpcMetricsSystem]
  }
}

@annotation.implicitNotFound(msg = "Cannot prove that ${A} <:!< ${B}.")
trait <:!<[A,B]
object <:!< {
  class Impl[A, B]
  object Impl {
    implicit def nsub[A, B] : A Impl B = null
    implicit def nsubAmbig1[A, B>:A] : A Impl B = null
    implicit def nsubAmbig2[A, B>:A] : A Impl B = null
  }

  implicit def foo[A,B]( implicit e: A Impl B ): A <:!< B = null
}

case class ReceiverMetricSystemBuilder(
                                        val sparkEnv: SparkEnv,
                                        val endpointName: String = RpcMetricsReceiver.DefaultEndpointName
                                      ) {
  require(sparkEnv != null, "sparkEnv cannot be null")
  require(!endpointName.isNullOrEmpty, "endpointName cannot be null, empty, or only whitespace")

  if (sparkEnv.executorId != SparkContext.DRIVER_IDENTIFIER) {
    throw new IllegalStateException("ReceiverMetricSystemBuilder can only be used on a driver")
  }

  private val metricsSources = mutable.Map[String, MetricsSource]()

  def addSource(namespace: String, create: (LocalMetricsSourceBuilder) => Unit): ReceiverMetricSystemBuilder = {
    val builder = new LocalMetricsSourceBuilder(namespace)
    create(builder)
    val metricsSource = builder.build
    metricsSources += (metricsSource.sourceName -> metricsSource)
    this
  }

  def build(): Unit = {
    this.metricsSources.values.foreach(
      source => this.sparkEnv.metricsSystem.registerSource(source)
    )

    this.sparkEnv.rpcEnv.setupEndpoint(
      this.endpointName,
      new RpcMetricsReceiver(
        this.sparkEnv,
        this.metricsSources.values.toSeq)
    )
  }
}