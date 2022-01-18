package org.apache.spark.metrics

import org.apache.spark._
import org.apache.spark.rpc.RpcEnv
import org.mockito.AdditionalAnswers
import org.mockito.ArgumentMatchers.{any, argThat}
import org.mockito.Mockito.{mock, times, verify, when}
import org.scalatest.BeforeAndAfterEach

import scala.reflect.ClassTag

object MetricsSystemsSuite {
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

  val GaugeName = "testrandomgauge"
  val InvalidCounterName = "invalidcounter"
}

class MetricsSystemsSuite extends SparkFunSuite
  with BeforeAndAfterEach
  with LocalSparkContext {

  private var env: SparkEnv = null
  private var rpcEnv: RpcEnv = null

  import TestImplicits._

  override def beforeEach(): Unit = {
    super.beforeEach
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("test")
      .set("spark.dynamicAllocation.testing", "true")
      .set("spark.driver.allowMultipleContexts", "true")
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
    sc=null
    env = null
    rpcEnv = null
  }

  test("getMetricsSystem registers a MetricsSource and returns a LocalMetricsSystem on driver node") {
    val envMetricsSystem = mock(classOf[org.apache.spark.metrics.MetricsSystem])
    when(env.metricsSystem).thenReturn(envMetricsSystem)
    when(env.executorId).thenReturn(SparkContext.DRIVER_IDENTIFIER)
    val metricsSystem = UserMetricsSystems.getMetricSystem(
      MetricsSystemsSuite.DriverMetricNamespace,
      (builder) => {
        builder.registerCounter(MetricsSystemsSuite.CounterName)
      }
    )

    assert(metricsSystem !== null)
    verify(envMetricsSystem, times(1)).registerSource(
      argThat((source: org.apache.spark.metrics.source.Source) => source.metricRegistry.counter(
        MetricsSystemsSuite.CounterName
      ) != null))
  }

  test("getMetricsSystem registers a MetricsSource and returns a LocalMetricsSystem on executor node") {
    val envMetricsSystem = mock(classOf[org.apache.spark.metrics.MetricsSystem])
    when(env.metricsSystem).thenReturn(envMetricsSystem)
    when(env.executorId).thenReturn("0")
    val metricsSystem = UserMetricsSystems.getMetricSystem(
      MetricsSystemsSuite.ExecutorMetricNamespace,
      (builder) => {
        builder.registerCounter(MetricsSystemsSuite.CounterName)
      }
    )

    assert(metricsSystem !== null)
    verify(envMetricsSystem, times(1)).registerSource(
      argThat((source: org.apache.spark.metrics.source.Source) => source.metricRegistry.counter(
        MetricsSystemsSuite.CounterName
      ) != null))
  }

  def spyLambda[T <: AnyRef](realObj: T)(implicit classTag: ClassTag[T]): T = mock(
    classTag.runtimeClass.asInstanceOf[Class[T]],
    AdditionalAnswers.delegatesTo(realObj))

  test("buildReceiverMetricsSystem succeeds when invoked on a driver") {
    val lambda = spyLambda((builder: ReceiverMetricSystemBuilder) => {})
    when(env.executorId).thenReturn(SparkContext.DRIVER_IDENTIFIER)
    UserMetricsSystems.buildReceiverMetricSystem(
      env,
      lambda
    )

    verify(lambda, times(1)).apply(any(classOf[ReceiverMetricSystemBuilder]))
  }

  test("buildReceiverMetricsSystem throws an IllegalStateException when invoked on an executor") {
    when(env.executorId).thenReturn("0")
    val caught = intercept[IllegalStateException] {
      UserMetricsSystems.buildReceiverMetricSystem(
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
      UserMetricsSystems.getRemoteMetricSystem(
        MetricsSystemsSuite.MetricNamespace,
        (builder) => {}
      )
    }

    assert(caught !== null)
    assert(caught.getMessage == "getRemoteMetricSystem cannot be invoked on a Spark driver")
  }

  test("getRemoteMetricsSystem succeeds when invoked on an executor") {
    val lambda = spyLambda((builder: RemoteMetricsSourceBuilder) => {})
    when(env.executorId).thenReturn("0")
    val metricSystem = UserMetricsSystems.getRemoteMetricSystem(
      MetricsSystemsSuite.MetricNamespace,
      lambda
    )

    assert(metricSystem !== null)
    verify(lambda, times(1)).apply(any(classOf[RemoteMetricsSourceBuilder]))
  }
}
