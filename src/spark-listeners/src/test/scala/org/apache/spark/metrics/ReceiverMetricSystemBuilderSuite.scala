package org.apache.spark.metrics

import org.apache.spark._
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, argThat}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach

object ReceiverMetricSystemBuilderSuite {
  val MetricNamespace = "testmetrics"
  val CounterName = "testcounter"
  val EndpointName = "testendpoint"
}

class ReceiverMetricSystemBuilderSuite extends SparkFunSuite
  with BeforeAndAfterEach {
  import TestImplicits._

  private var env: SparkEnv = null
  private var rpcEnv: RpcEnv = null

  override def beforeEach(): Unit = {
    super.beforeEach
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("test")
      .set("spark.dynamicAllocation.testing", "true")
      .set("spark.driver.allowMultipleContexts", "true")
    env = mock(classOf[SparkEnv])
    rpcEnv = mock(classOf[RpcEnv])
    when(env.executorId).thenReturn(SparkContext.DRIVER_IDENTIFIER)
    when(env.conf).thenReturn(conf)
    when(env.rpcEnv).thenReturn(rpcEnv)
    SparkEnv.set(env)
  }

  override def afterEach(): Unit = {
    super.afterEach
    env = null
    rpcEnv = null
  }

  test("sparkEnv cannot be null") {
    val caught = intercept[IllegalArgumentException] {
      val builder = new ReceiverMetricSystemBuilder(null)
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("sparkEnv cannot be null"))
  }

  test("endpointName cannot be null") {
    val caught = intercept[IllegalArgumentException] {
      val builder = new ReceiverMetricSystemBuilder(env, null)
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("endpointName cannot be null, empty, or only whitespace"))
  }

  test("endpointName cannot be empty") {
    val caught = intercept[IllegalArgumentException] {
      val builder = new ReceiverMetricSystemBuilder(env, "")
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("endpointName cannot be null, empty, or only whitespace"))
  }

  test("endpointName cannot be only whitespace") {
    val caught = intercept[IllegalArgumentException] {
      val builder = new ReceiverMetricSystemBuilder(env, "   ")
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("endpointName cannot be null, empty, or only whitespace"))
  }

  test("ReceiverMetricSystemBuilder cannot be used on executors") {
    when(env.executorId).thenReturn("0")
    val caught = intercept[IllegalStateException] {
      val builder = new ReceiverMetricSystemBuilder(env, RpcMetricsReceiver.DefaultEndpointName)
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("ReceiverMetricSystemBuilder can only be used on a driver"))
  }

  test("build() registers one metrics source") {
    val metricsSystem = mock(classOf[org.apache.spark.metrics.MetricsSystem])
    when(env.metricsSystem).thenReturn(metricsSystem)
    when(rpcEnv.setupEndpoint(any[String], any[RpcEndpoint])).thenReturn(mock(classOf[RpcEndpointRef]))

    val builder = new ReceiverMetricSystemBuilder(env, ReceiverMetricSystemBuilderSuite.EndpointName)
    builder.addSource(ReceiverMetricSystemBuilderSuite.MetricNamespace, builder => {
      builder.registerCounter(ReceiverMetricSystemBuilderSuite.CounterName)
    })
    builder.build
    ///verify(this.rpcMetricsReceiverRef).send(argThat((message: CounterMessage) => message.value === 1))
    verify(metricsSystem, times(1)).registerSource(
      argThat((source: org.apache.spark.metrics.source.Source) => source.metricRegistry.counter(
        ReceiverMetricSystemBuilderSuite.CounterName
      ) != null))
    verify(rpcEnv, times(1)).setupEndpoint(ArgumentMatchers.eq(
      ReceiverMetricSystemBuilderSuite.EndpointName), any[RpcMetricsReceiver]
    )
  }
}