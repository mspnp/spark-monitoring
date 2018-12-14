package org.apache.spark.metrics.microsoft.practices

import org.apache.spark._
import org.apache.spark.rpc.RpcEnv
import org.mockito.Matchers.{any, argThat}
import org.mockito.Mockito.{mock, times, verify, when}
import org.scalatest.BeforeAndAfterEach

import org.mockito.AdditionalAnswers

import scala.reflect.ClassTag

object CustomMetricsSystemSuite {
  val MetricNamespace = "testmetrics"
  val DriverMetricNamespace = "testdrivermetrics"
  val ExecutorMetricNamespace = "testexecutormetrics"
  val CustomMetricNamespace = "custommetrics"
  val CounterName = "testcounter"
  val HistogramName = "testhistogram"
  val MeterName = "testmeter"
  val TimerName = "testtimer"
  val SettableGaugeName = "testsettablegauge"

  val NamespaceFieldName = "namespace"
  val EndpointNameFieldName = "endpointName"
}

class CustomMetricsSystemSuite extends SparkFunSuite
  with BeforeAndAfterEach
  with LocalSparkContext {

  private var env: SparkEnv = null
  private var rpcEnv: RpcEnv = null

  override def beforeEach(): Unit = {
    super.beforeEach
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("test")
      .set("spark.dynamicAllocation.testing", "true")
    sc = new SparkContext(conf)
    env = mock(classOf[SparkEnv])
    rpcEnv = mock(classOf[RpcEnv])
    when(env.conf).thenReturn(conf)
    when(env.rpcEnv).thenReturn(rpcEnv)
    SparkEnv.set(env)
    //when(sc.env).thenReturn(env)
  }

  override def afterEach(): Unit = {
    super.afterEach
    env = null
    rpcEnv = null
  }

  test("getMetricsSystem registers a MetricsSource and returns a LocalMetricsSystem on driver node") {
    val envMetricsSystem = mock(classOf[org.apache.spark.metrics.MetricsSystem])
    when(env.metricsSystem).thenReturn(envMetricsSystem)
    when(env.executorId).thenReturn(SparkContext.DRIVER_IDENTIFIER)
    val metricsSystem = CustomMetricsSystem.getMetricSystem(
      CustomMetricsSystemSuite.DriverMetricNamespace,
      (builder) => {
        builder.registerCounter(CustomMetricsSystemSuite.CounterName)
      }
    )

    assert(metricsSystem !== null)
    import TestImplicits.matcher
    verify(envMetricsSystem, times(1)).registerSource(
      argThat((source: org.apache.spark.metrics.source.Source) => source.metricRegistry.counter(
        CustomMetricsSystemSuite.CounterName
      ) != null))
  }

  test("getMetricsSystem registers a MetricsSource and returns a LocalMetricsSystem on executor node") {
    val envMetricsSystem = mock(classOf[org.apache.spark.metrics.MetricsSystem])
    when(env.metricsSystem).thenReturn(envMetricsSystem)
    when(env.executorId).thenReturn("0")
    val metricsSystem = CustomMetricsSystem.getMetricSystem(
      CustomMetricsSystemSuite.ExecutorMetricNamespace,
      (builder) => {
        builder.registerCounter(CustomMetricsSystemSuite.CounterName)
      }
    )

    assert(metricsSystem !== null)
    import TestImplicits.matcher
    verify(envMetricsSystem, times(1)).registerSource(
      argThat((source: org.apache.spark.metrics.source.Source) => source.metricRegistry.counter(
        CustomMetricsSystemSuite.CounterName
      ) != null))
  }

  def spyLambda[T <: AnyRef](realObj: T)(implicit classTag: ClassTag[T]): T = mock(
    classTag.runtimeClass.asInstanceOf[Class[T]],
    AdditionalAnswers.delegatesTo(realObj))

  test("buildReceiverMetricsSystem succeeds when invoked on a driver") {
    val lambda = spyLambda((builder: ReceiverMetricSystemBuilder) => {})
    when(env.executorId).thenReturn(SparkContext.DRIVER_IDENTIFIER)
    CustomMetricsSystem.buildReceiverMetricSystem(
      env,
      lambda
    )

    verify(lambda, times(1)).apply(any(classOf[ReceiverMetricSystemBuilder]))
  }

  test("buildReceiverMetricsSystem throws an IllegalStateException when invoked on an executor") {
    when(env.executorId).thenReturn("0")
    val caught = intercept[IllegalStateException] {
      CustomMetricsSystem.buildReceiverMetricSystem(
        env,
        (builder) => {}
      )
    }

    assert(caught !== null)
    assert(caught.getMessage == "buildReceiverMetricSystem cannot be invoked on a Spark executor")
  }

  test("getRemoteMetricsSystem throws an IllegalStateException when invoked on a driver") {
    when(env.executorId).thenReturn(SparkContext.DRIVER_IDENTIFIER)
    val caught = intercept[IllegalStateException] {
      CustomMetricsSystem.getRemoteMetricSystem(
        CustomMetricsSystemSuite.MetricNamespace,
        (builder) => {}
      )
    }

    assert(caught !== null)
    assert(caught.getMessage == "getRemoteMetricSystem cannot be invoked on a Spark driver")
  }

  test("getRemoteMetricsSystem succeeds when invoked on an executor") {
    val lambda = spyLambda((builder: RemoteMetricsSourceBuilder) => {})
    when(env.executorId).thenReturn("0")
    val metricSystem = CustomMetricsSystem.getRemoteMetricSystem(
      CustomMetricsSystemSuite.MetricNamespace,
      lambda
    )

    assert(metricSystem !== null)
    verify(lambda, times(1)).apply(any(classOf[RemoteMetricsSourceBuilder]))
  }
}
