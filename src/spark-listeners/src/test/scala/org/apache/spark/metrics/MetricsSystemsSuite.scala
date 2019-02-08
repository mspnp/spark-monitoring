package org.apache.spark.metrics

import com.codahale.metrics._
import org.apache.spark._
import org.mockito.Mockito.mock
import org.scalatest.BeforeAndAfterEach

//object MetricsSystemsSuite {
//  val MetricNamespace = "testmetrics"
//  val CounterName = "testcounter"
//  val HistogramName = "testhistogram"
//  val MeterName = "testmeter"
//  val TimerName = "testtimer"
//  val SettableGaugeName = "testsettablegauge"
//  val GaugeName = "testrandomgauge"
//  val InvalidCounterName = "invalidcounter"
//}

class LocalMetricsSystemsSuite extends SparkFunSuite
  with BeforeAndAfterEach {

  private var counter: Counter = null
  private var histogram: Histogram = null
  private var meter: Meter = null
  private var timer: Timer = null
  private var settableGauge: SettableGauge[Long] = null
  private var gauge: Gauge[Long] = null
  private var metricsSource: MetricsSource = null

  override def beforeEach(): Unit = {
    super.beforeEach
    this.counter = mock(classOf[Counter])
    this.histogram = mock(classOf[Histogram])
    this.meter = mock(classOf[Meter])
    this.timer = mock(classOf[Timer])
    this.settableGauge = mock(classOf[SettableGauge[Long]])
    this.gauge = mock(classOf[Gauge[Long]])
    val metricRegistry = new MetricRegistry()
    metricRegistry.register(MetricsSystemsSuite.CounterName, this.counter)
    metricRegistry.register(MetricsSystemsSuite.HistogramName, this.histogram)
    metricRegistry.register(MetricsSystemsSuite.MeterName, this.meter)
    metricRegistry.register(MetricsSystemsSuite.TimerName, this.timer)
    metricRegistry.register(MetricsSystemsSuite.SettableGaugeName, this.settableGauge)
    metricRegistry.register(MetricsSystemsSuite.GaugeName, this.gauge)
    this.metricsSource = new MetricsSource(MetricsSystemsSuite.MetricNamespace, metricRegistry)
  }

  override def afterEach(): Unit = {
    super.afterEach
    this.counter = null
    this.histogram = null
    this.meter = null
    this.timer = null
    this.settableGauge = null
    this.gauge = null
  }

  test("metricsSource cannot be null") {
    val caught = intercept[IllegalArgumentException] {
      val metricsSystem = new LocalMetricsSystem(null)
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("metricsSource cannot be null"))
  }

  test("counter() returns named counter") {
    val metricsSystem = new LocalMetricsSystem(this.metricsSource)
    val metric = metricsSystem.counter(MetricsSystemsSuite.CounterName)
    assert(metric !== null)
  }

  test("histogram() returns named histogram") {
    val metricsSystem = new LocalMetricsSystem(this.metricsSource)
    val metric = metricsSystem.histogram(MetricsSystemsSuite.HistogramName)
    assert(metric !== null)
  }

  test("meter() returns named meter") {
    val metricsSystem = new LocalMetricsSystem(this.metricsSource)
    val metric = metricsSystem.meter(MetricsSystemsSuite.MeterName)
    assert(metric !== null)
  }

  test("timer() returns named timer") {
    val metricsSystem = new LocalMetricsSystem(this.metricsSource)
    val metric = metricsSystem.timer(MetricsSystemsSuite.TimerName)
    assert(metric !== null)
  }

  test("gauge() returns named gauge") {
    val metricsSystem = new LocalMetricsSystem(this.metricsSource)
    val metric = metricsSystem.gauge(MetricsSystemsSuite.SettableGaugeName)
    assert(metric !== null)
  }

  test("gauge() throws a SparkException for a gauge that does not inherit from SettableGauge[T]") {
    val metricsSystem = new LocalMetricsSystem(this.metricsSource)
    val caught = intercept[SparkException] {
      val metric = metricsSystem.gauge(MetricsSystemsSuite.GaugeName)
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("does not extend SettableGauge[T]"))
  }

  test("counter() throws a SparkException for a counter that does not exist") {
    val metricsSystem = new LocalMetricsSystem(this.metricsSource)
    val caught = intercept[SparkException] {
      val metric = metricsSystem.counter(MetricsSystemsSuite.InvalidCounterName)
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("was not found"))
  }
}

class RpcMetricsSystemsSuite extends SparkFunSuite
  with BeforeAndAfterEach {

  private var counter: CounterProxy = null
  private var histogram: HistogramProxy = null
  private var meter: MeterProxy = null
  private var timer: TimerProxy = null
  private var settableGauge: SettableGaugeProxy[Long] = null
  private var metricsSource: MetricsSource = null

  override def beforeEach(): Unit = {
    super.beforeEach
    this.counter = mock(classOf[CounterProxy])
    this.histogram = mock(classOf[HistogramProxy])
    this.meter = mock(classOf[MeterProxy])
    this.timer = mock(classOf[TimerProxy])
    this.settableGauge = mock(classOf[SettableGaugeProxy[Long]])
    val metricRegistry = new MetricRegistry()
    metricRegistry.register(MetricsSystemsSuite.CounterName, this.counter)
    metricRegistry.register(MetricsSystemsSuite.HistogramName, this.histogram)
    metricRegistry.register(MetricsSystemsSuite.MeterName, this.meter)
    metricRegistry.register(MetricsSystemsSuite.TimerName, this.timer)
    metricRegistry.register(MetricsSystemsSuite.SettableGaugeName, this.settableGauge)
    this.metricsSource = new MetricsSource(MetricsSystemsSuite.MetricNamespace, metricRegistry)
  }

  override def afterEach(): Unit = {
    super.afterEach
    this.counter = null
    this.histogram = null
    this.meter = null
    this.timer = null
    this.settableGauge = null
  }

  test("metricsSource cannot be null") {
    val caught = intercept[IllegalArgumentException] {
      val metricsSystem = new RpcMetricsSystem(null)
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("metricsSource cannot be null"))
  }

  test("counter() returns named counter") {
    val metricsSystem = new RpcMetricsSystem(this.metricsSource)
    val metric = metricsSystem.counter(MetricsSystemsSuite.CounterName)
    assert(metric !== null)
  }

  test("histogram() returns named histogram") {
    val metricsSystem = new RpcMetricsSystem(this.metricsSource)
    val metric = metricsSystem.histogram(MetricsSystemsSuite.HistogramName)
    assert(metric !== null)
  }

  test("meter() returns named meter") {
    val metricsSystem = new RpcMetricsSystem(this.metricsSource)
    val metric = metricsSystem.meter(MetricsSystemsSuite.MeterName)
    assert(metric !== null)
  }

  test("timer() returns named timer") {
    val metricsSystem = new RpcMetricsSystem(this.metricsSource)
    val metric = metricsSystem.timer(MetricsSystemsSuite.TimerName)
    assert(metric !== null)
  }

  test("gauge() returns named gauge") {
    val metricsSystem = new RpcMetricsSystem(this.metricsSource)
    val metric = metricsSystem.gauge(MetricsSystemsSuite.SettableGaugeName)
    assert(metric !== null)
  }

  test("counter() throws a SparkException for a counter that does not exist") {
    val metricsSystem = new RpcMetricsSystem(this.metricsSource)
    val caught = intercept[SparkException] {
      val metric = metricsSystem.counter(MetricsSystemsSuite.InvalidCounterName)
    }

    assert(caught !== null)
    assert(caught.getMessage.contains("was not found"))
  }
}
