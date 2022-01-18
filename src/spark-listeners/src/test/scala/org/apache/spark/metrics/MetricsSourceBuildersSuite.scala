package org.apache.spark.metrics

import com.codahale.metrics._
import org.apache.spark._
import org.apache.spark.rpc.{RpcAddress, RpcEnv}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doAnswer, mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterEach

object MetricsSourceBuildersSuite {
  val MetricNamespace = "testmetrics"
  val CounterName = "testcounter"
  val HistogramName = "testhistogram"
  val MeterName = "testmeter"
  val TimerName = "testtimer"
  val SettableGaugeName = "testsettablegauge"
  val CustomHistogramName = "testcustomhistogram"
}

class LocalMetricsSourceBuilderSuite extends SparkFunSuite
  with BeforeAndAfterEach {
  override def beforeEach(): Unit = {
    super.beforeEach
  }

  override def afterEach(): Unit = {
    super.afterEach
  }

  test("namespace cannot be null") {
    val caught = intercept[IllegalArgumentException] {
      val builder = new LocalMetricsSourceBuilder(null)
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("namespace cannot be null, empty, or only whitespace"))
  }

  test("namespace cannot be empty") {
    val caught = intercept[IllegalArgumentException] {
      val builder = new LocalMetricsSourceBuilder("")
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("namespace cannot be null, empty, or only whitespace"))
  }

  test("namespace cannot be only whitespace") {
    val caught = intercept[IllegalArgumentException] {
      val builder = new LocalMetricsSourceBuilder("   ")
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("namespace cannot be null, empty, or only whitespace"))
  }

  test("registerCounter adds named Counter") {
    val builder = new LocalMetricsSourceBuilder(MetricsSourceBuildersSuite.MetricNamespace)
    builder.registerCounter(MetricsSourceBuildersSuite.CounterName)

    val source = builder.build
    assert(source.sourceName === MetricsSourceBuildersSuite.MetricNamespace)
    val metric = source.metricRegistry.counter(MetricsSourceBuildersSuite.CounterName)
    assert(metric !== null)
    assert(metric.isInstanceOf[Counter])
  }

  test("registerHistogram adds named Histogram") {
    val builder = new LocalMetricsSourceBuilder(MetricsSourceBuildersSuite.MetricNamespace)
    builder.registerHistogram(MetricsSourceBuildersSuite.HistogramName)

    val source = builder.build
    val metric = source.metricRegistry.histogram(MetricsSourceBuildersSuite.HistogramName)
    assert(source.sourceName === MetricsSourceBuildersSuite.MetricNamespace)
    assert(metric !== null)
    assert(metric.isInstanceOf[Histogram])
  }

  test("registerMeter adds named Meter") {
    val builder = new LocalMetricsSourceBuilder(MetricsSourceBuildersSuite.MetricNamespace)
    builder.registerMeter(MetricsSourceBuildersSuite.MeterName)

    val source = builder.build
    val metric = source.metricRegistry.meter(MetricsSourceBuildersSuite.MeterName)
    assert(source.sourceName === MetricsSourceBuildersSuite.MetricNamespace)
    assert(metric !== null)
    assert(metric.isInstanceOf[Meter])
  }

  test("registerTimer adds named Timer") {
    val builder = new LocalMetricsSourceBuilder(MetricsSourceBuildersSuite.MetricNamespace)
    builder.registerTimer(MetricsSourceBuildersSuite.TimerName)

    val source = builder.build
    val metric = source.metricRegistry.timer(MetricsSourceBuildersSuite.TimerName)
    assert(source.sourceName === MetricsSourceBuildersSuite.MetricNamespace)
    assert(metric !== null)
    assert(metric.isInstanceOf[Timer])
  }

  test("metric name cannot be null") {
    val caught = intercept[IllegalArgumentException] {
      val builder = new LocalMetricsSourceBuilder(MetricsSourceBuildersSuite.MetricNamespace)
      builder.register(null, new Histogram(new UniformReservoir))
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("name cannot be null, empty, or only whitespace"))
  }

  test("metric name cannot be empty") {
    val caught = intercept[IllegalArgumentException] {
      val builder = new LocalMetricsSourceBuilder(MetricsSourceBuildersSuite.MetricNamespace)
      builder.register("", new Histogram(new UniformReservoir))
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("name cannot be null, empty, or only whitespace"))
  }

  test("metric name cannot be only whitespace") {
    val caught = intercept[IllegalArgumentException] {
      val builder = new LocalMetricsSourceBuilder(MetricsSourceBuildersSuite.MetricNamespace)
      builder.register("   ", new Histogram(new UniformReservoir))
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("name cannot be null, empty, or only whitespace"))
  }

  test("register adds named metric") {
    val builder = new LocalMetricsSourceBuilder(MetricsSourceBuildersSuite.MetricNamespace)
    builder.register(MetricsSourceBuildersSuite.CustomHistogramName, new Histogram(new UniformReservoir))
    val source = builder.build
    val metric = source.metricRegistry.histogram(MetricsSourceBuildersSuite.CustomHistogramName)
    assert(metric !== null)
  }
}

class RemoteMetricsSourceBuilderSuite extends SparkFunSuite
  with BeforeAndAfterEach {

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
    when(env.conf).thenReturn(conf)
    when(env.rpcEnv).thenReturn(rpcEnv)
    SparkEnv.set(env)
  }

  override def afterEach(): Unit = {
    super.afterEach
    env = null
    rpcEnv = null
  }

  test("namespace cannot be null") {
    val caught = intercept[IllegalArgumentException] {
      val builder = new RemoteMetricsSourceBuilder(null, RpcMetricsReceiver.DefaultEndpointName,
        env)
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("namespace cannot be null, empty, or only whitespace"))
  }

  test("namespace cannot be empty") {
    val caught = intercept[IllegalArgumentException] {
      val builder = new RemoteMetricsSourceBuilder("", RpcMetricsReceiver.DefaultEndpointName,
        env)
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("namespace cannot be null, empty, or only whitespace"))
  }

  test("namespace cannot be only whitespace") {
    val caught = intercept[IllegalArgumentException] {
      val builder = new RemoteMetricsSourceBuilder("   ", RpcMetricsReceiver.DefaultEndpointName,
        env)
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("namespace cannot be null, empty, or only whitespace"))
  }

  test("RemoteMetricsSourceBuilder throws an IllegalStateException when used on a driver") {
    when(env.executorId).thenReturn(SparkContext.DRIVER_IDENTIFIER)
    val caught = intercept[IllegalStateException] {
      val builder = new RemoteMetricsSourceBuilder(
        MetricsSourceBuildersSuite.MetricNamespace,
        RpcMetricsReceiver.DefaultEndpointName,
        env)
    }

    assert(caught !== null)
    assert(caught.getMessage == "RemoteMetricsSourceBuilder cannot be used on a driver")
  }

  test("RpcUtils.makeDriverRef throws a SparkException") {
    doAnswer(new Answer[Void] {
      override def answer(invocation: InvocationOnMock): Void = {
        throw new SparkException("something bad happened")
      }
    }).when(rpcEnv).setupEndpointRef(any(classOf[RpcAddress]), any(classOf[String]))
    //    when(env.executorId).thenReturn(SparkContext.DRIVER_IDENTIFIER)
    val caught = intercept[SparkException] {
      val builder = new RemoteMetricsSourceBuilder(
        MetricsSourceBuildersSuite.MetricNamespace,
        RpcMetricsReceiver.DefaultEndpointName,
        env)
    }

    assert(caught !== null)
  }

  test("valid namespace") {
    val builder = new RemoteMetricsSourceBuilder(
      MetricsSourceBuildersSuite.MetricNamespace,
      RpcMetricsReceiver.DefaultEndpointName,
      env)

    assert(builder !== null)
  }

  test("registerCounter adds named CounterProxy") {
    val builder = new RemoteMetricsSourceBuilder(
      MetricsSourceBuildersSuite.MetricNamespace,
      RpcMetricsReceiver.DefaultEndpointName,
      env
    )
    builder.registerCounter(MetricsSourceBuildersSuite.CounterName)

    val source = builder.build
    assert(source.sourceName === MetricsSourceBuildersSuite.MetricNamespace)
    val metric = source.metricRegistry.counter(MetricsSourceBuildersSuite.CounterName)
    assert(metric !== null)
    assert(metric.isInstanceOf[CounterProxy])
  }

  test("registerHistogram adds named HistogramProxy") {
    val builder = new RemoteMetricsSourceBuilder(
      MetricsSourceBuildersSuite.MetricNamespace,
      RpcMetricsReceiver.DefaultEndpointName,
      env
    )
    builder.registerHistogram(MetricsSourceBuildersSuite.HistogramName)

    val source = builder.build
    val metric = source.metricRegistry.histogram(MetricsSourceBuildersSuite.HistogramName)
    assert(source.sourceName === MetricsSourceBuildersSuite.MetricNamespace)
    assert(metric !== null)
    assert(metric.isInstanceOf[HistogramProxy])
  }

  test("registerMeter adds named MeterProxy") {
    val builder = new RemoteMetricsSourceBuilder(
      MetricsSourceBuildersSuite.MetricNamespace,
      RpcMetricsReceiver.DefaultEndpointName,
      env
    )
    builder.registerMeter(MetricsSourceBuildersSuite.MeterName)

    val source = builder.build
    val metric = source.metricRegistry.meter(MetricsSourceBuildersSuite.MeterName)
    assert(source.sourceName === MetricsSourceBuildersSuite.MetricNamespace)
    assert(metric !== null)
    assert(metric.isInstanceOf[MeterProxy])
  }

  test("registerTimer adds named TimerProxy") {
    val builder = new RemoteMetricsSourceBuilder(
      MetricsSourceBuildersSuite.MetricNamespace,
      RpcMetricsReceiver.DefaultEndpointName,
      env
    )
    builder.registerTimer(MetricsSourceBuildersSuite.TimerName)

    val source = builder.build
    val metric = source.metricRegistry.timer(MetricsSourceBuildersSuite.TimerName)
    assert(source.sourceName === MetricsSourceBuildersSuite.MetricNamespace)
    assert(metric !== null)
    assert(metric.isInstanceOf[TimerProxy])
  }

  test("metric name cannot be null") {
    val caught = intercept[IllegalArgumentException] {
      val builder = new RemoteMetricsSourceBuilder(
        MetricsSourceBuildersSuite.MetricNamespace,
        RpcMetricsReceiver.DefaultEndpointName,
        env
      )
      //builder.register(null, new Histogram(new UniformReservoir))
      builder.register(null, new HistogramProxy(
        builder.endpointRef,
        builder.namespace,
        null,
        new UniformReservoir
      ))
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("name cannot be null, empty, or only whitespace"))
  }

  test("metric name cannot be empty") {
    val caught = intercept[IllegalArgumentException] {
      val builder = new RemoteMetricsSourceBuilder(
        MetricsSourceBuildersSuite.MetricNamespace,
        RpcMetricsReceiver.DefaultEndpointName,
        env
      )
      builder.register("", new HistogramProxy(
        builder.endpointRef,
        builder.namespace,
        "",
        new UniformReservoir
      ))
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("name cannot be null, empty, or only whitespace"))
  }

  test("metric name cannot be only whitespace") {
    val caught = intercept[IllegalArgumentException] {
      val builder = new RemoteMetricsSourceBuilder(
        MetricsSourceBuildersSuite.MetricNamespace,
        RpcMetricsReceiver.DefaultEndpointName,
        env
      )
      builder.register("   ", new HistogramProxy(
        builder.endpointRef,
        builder.namespace,
        "   ",
        new UniformReservoir
      ))
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("name cannot be null, empty, or only whitespace"))
  }

  test("register adds named metric") {
    val builder = new RemoteMetricsSourceBuilder(
      MetricsSourceBuildersSuite.MetricNamespace,
      RpcMetricsReceiver.DefaultEndpointName,
      env
    )
    builder.register(
      MetricsSourceBuildersSuite.CustomHistogramName,
      new HistogramProxy(
        builder.endpointRef,
        builder.namespace,
        MetricsSourceBuildersSuite.CustomHistogramName,
        new UniformReservoir))
    val source = builder.build
    val metric = source.metricRegistry.histogram(MetricsSourceBuildersSuite.CustomHistogramName)
    assert(metric !== null)
  }
}
