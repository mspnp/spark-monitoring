package org.apache.spark.metrics

import java.util.concurrent.TimeUnit

import com.codahale.metrics._
import org.apache.spark._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterEach, PrivateMethodTester}
import TestUtils._

class CustomMetric extends Metric {
  def foo(value: Int) = {
  }
}

class DoubleCounter extends Counter {
  override def inc(n: Long): Unit = {
    super.inc(n * 2)
  }
}

object RpcMetricsReceiverSuite {
  val MetricNamespace = "testmetrics"
  val CounterName = "testcounter"
  val HistogramName = "testhistogram"
  val MeterName = "testmeter"
  val TimerName = "testtimer"
  val SettableGaugeName = "testsettablegauge"
  val InvalidCounterName = "invalidcounter"
  val InvalidHistogramName = "invalidhistogramname"
  val InvalidMeterName = "invalidmeter"
  val InvalidTimerName = "invalidtimer"
  val InvalidSettableGaugeName = "invalidsettablegauge"
  val CustomName = "testcustom"
  val DerivedCounterName = "testdoublecounter"
  val DefaultRpcEventLoopTimeout = 1000
}

class RpcMetricsReceiverSuite extends SparkFunSuite
  with BeforeAndAfterEach
  with PrivateMethodTester
  with LocalSparkContext {

  private val executorId1 = "1"
  private val executorId2 = "2"

  // Shared state that must be reset before and after each test
  private var scheduler: TaskSchedulerImpl = null
  private var rpcMetricsReceiver: RpcMetricsReceiver = null
  private var rpcMetricsReceiverRef: RpcEndpointRef = null
  private var counter: Counter = null
  private var histogram: Histogram = null
  private var meter: Meter = null
  private var timer: Timer = null
  private var settableGauge: SettableGauge[Long] = null

  val clockClazz = loadOneOf("com.codahale.metrics.jvm.CpuTimeClock","com.codahale.metrics.Clock$CpuTimeClock").get
    .asInstanceOf[Class[_<:Clock]]

  /**
    * Before each test, set up the SparkContext and a custom [[RpcMetricsReceiver]]
    * that uses a manual clock.
    */
  override def beforeEach(): Unit = {
    super.beforeEach()
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("test")
      .set("spark.dynamicAllocation.testing", "true")
      .set("spark.driver.allowMultipleContexts", "true")
    sc = spy(new SparkContext(conf))
    scheduler = mock(classOf[TaskSchedulerImpl])
    when(sc.taskScheduler).thenReturn(scheduler)
    when(scheduler.sc).thenReturn(sc)
    val metricRegistry = new MetricRegistry()
    counter = mock(classOf[Counter])
    //doNothing.when(counter).inc(any[Long])
    histogram = spy(new Histogram(new ExponentiallyDecayingReservoir))
    meter = spy(new Meter(new Clock.UserTimeClock))
    timer = spy(new Timer(new ExponentiallyDecayingReservoir, new Clock.UserTimeClock))
    settableGauge = mock(classOf[SettableGauge[Long]])
    metricRegistry.register(RpcMetricsReceiverSuite.CounterName, counter)
    metricRegistry.register(RpcMetricsReceiverSuite.HistogramName, histogram)
    metricRegistry.register(RpcMetricsReceiverSuite.MeterName, meter)
    metricRegistry.register(RpcMetricsReceiverSuite.TimerName, timer)
    metricRegistry.register(RpcMetricsReceiverSuite.SettableGaugeName, settableGauge)
    metricRegistry.register(RpcMetricsReceiverSuite.CustomName, new CustomMetric)
    metricRegistry.register(RpcMetricsReceiverSuite.DerivedCounterName, new DoubleCounter)
    val metricsSource = new MetricsSource(RpcMetricsReceiverSuite.MetricNamespace, metricRegistry)
    rpcMetricsReceiver = spy(new RpcMetricsReceiver(sc.env, Seq(metricsSource)))
    rpcMetricsReceiverRef = sc.env.rpcEnv.setupEndpoint(RpcMetricsReceiver.DefaultEndpointName, rpcMetricsReceiver)
  }

  /**
    * After each test, clean up all state and stop the [[SparkContext]].
    */
  override def afterEach(): Unit = {
    super.afterEach()
    scheduler = null
    rpcMetricsReceiver = null
    rpcMetricsReceiverRef = null
    counter = null
    histogram = null
    meter = null
    timer = null
    settableGauge = null
    sc=null
  }

  test("getMetric returns valid metric") {
    val metric = rpcMetricsReceiver.getMetric[Counter](
      RpcMetricsReceiverSuite.MetricNamespace,
      RpcMetricsReceiverSuite.CounterName)
    assert(metric !== None)
  }

  test("getMetric returns None for non-existent namespace") {
    val metric = rpcMetricsReceiver.getMetric[Counter](
      "invalidnamespace",
      RpcMetricsReceiverSuite.CounterName)
    assert(metric === None)
  }

  test("getMetric returns None for non-existent metric") {
    val metric = rpcMetricsReceiver.getMetric[Counter](
      RpcMetricsReceiverSuite.MetricNamespace,
      RpcMetricsReceiverSuite.InvalidCounterName)
    assert(metric === None)
  }

  test("getMetric returns None for valid namespace and metric, but invalid type") {
    val metric: Option[Histogram] = rpcMetricsReceiver.getMetric[Histogram](
      RpcMetricsReceiverSuite.MetricNamespace,
      RpcMetricsReceiverSuite.CounterName)
    assert(metric === None)
  }

  test("getMetric returns Metric subclass") {
    val metric = rpcMetricsReceiver.getMetric[CustomMetric](
      RpcMetricsReceiverSuite.MetricNamespace,
      RpcMetricsReceiverSuite.CustomName)
    assert(metric !== None)
  }

  test("getMetric returns Counter subclass") {
    val metric = rpcMetricsReceiver.getMetric[DoubleCounter](
      RpcMetricsReceiverSuite.MetricNamespace,
      RpcMetricsReceiverSuite.DerivedCounterName)
    assert(metric !== None)
  }

  test("CounterMessage received with valid name and inc() called"){
    val value: Long = 12345L
    rpcMetricsReceiverRef.send(CounterMessage(
      RpcMetricsReceiverSuite.MetricNamespace,
      RpcMetricsReceiverSuite.CounterName,
      value
    ))
    verify(counter, timeout(RpcMetricsReceiverSuite.DefaultRpcEventLoopTimeout)).inc(ArgumentMatchers.eq(value))
  }

  test("CounterMessage received with invalid name and inc() not called") {
    val value: Long = 12345L
    rpcMetricsReceiverRef.send(CounterMessage(
      RpcMetricsReceiverSuite.MetricNamespace,
      RpcMetricsReceiverSuite.InvalidCounterName,
      value
    ))

    verify(counter, timeout(RpcMetricsReceiverSuite.DefaultRpcEventLoopTimeout).times(0)).inc(any[Long])
  }

  test("HistogramMessage received with valid name and update() called"){
    val value: Long = 12345L
    rpcMetricsReceiverRef.send(HistogramMessage(
      RpcMetricsReceiverSuite.MetricNamespace,
      RpcMetricsReceiverSuite.HistogramName,
      value,
      classOf[ExponentiallyDecayingReservoir]
    ))
    verify(histogram, timeout(RpcMetricsReceiverSuite.DefaultRpcEventLoopTimeout)).update(ArgumentMatchers.eq(value))
  }

  test("HistogramMessage received with invalid name and update() not called") {
    val value: Long = 12345L
    rpcMetricsReceiverRef.send(HistogramMessage(
      RpcMetricsReceiverSuite.MetricNamespace,
      RpcMetricsReceiverSuite.InvalidHistogramName,
      value,
      classOf[ExponentiallyDecayingReservoir]
    ))
    verify(histogram, timeout(RpcMetricsReceiverSuite.DefaultRpcEventLoopTimeout).times(0)).update(any[Long])
  }

  test("HistogramMessage received with mismatched reservoir and update() not called") {
    val value: Long = 12345L
    rpcMetricsReceiverRef.send(HistogramMessage(
      RpcMetricsReceiverSuite.MetricNamespace,
      RpcMetricsReceiverSuite.HistogramName,
      value,
      classOf[UniformReservoir]
    ))
    verify(histogram, timeout(RpcMetricsReceiverSuite.DefaultRpcEventLoopTimeout).times(0)).update(any[Long])
  }

  test("MeterMessage received with valid name and mark() called"){
    val value: Long = 12345L
    rpcMetricsReceiverRef.send(MeterMessage(
      RpcMetricsReceiverSuite.MetricNamespace,
      RpcMetricsReceiverSuite.MeterName,
      value,
      classOf[Clock.UserTimeClock]
    ))
    verify(meter, timeout(RpcMetricsReceiverSuite.DefaultRpcEventLoopTimeout)).mark(ArgumentMatchers.eq(value))
  }

  test("MeterMessage received with invalid name and mark() not called") {
    val value: Long = 12345L
    rpcMetricsReceiverRef.send(MeterMessage(
      RpcMetricsReceiverSuite.MetricNamespace,
      RpcMetricsReceiverSuite.InvalidMeterName,
      value,
      classOf[Clock.UserTimeClock]
    ))
    verify(meter, timeout(RpcMetricsReceiverSuite.DefaultRpcEventLoopTimeout).times(0)).mark(any[Long])
  }

  test("MeterMessage received with mismatched clock and mark() not called") {
    val value: Long = 12345L
    rpcMetricsReceiverRef.send(MeterMessage(
      RpcMetricsReceiverSuite.MetricNamespace,
      RpcMetricsReceiverSuite.MeterName,
      value,
      clockClass = clockClazz
    ))
    verify(meter, timeout(RpcMetricsReceiverSuite.DefaultRpcEventLoopTimeout).times(0)).mark(any[Long])
  }

  test("TimerMessage received with valid name and update() called"){
    val value: Long = 12345L
    rpcMetricsReceiverRef.send(TimerMessage(
      RpcMetricsReceiverSuite.MetricNamespace,
      RpcMetricsReceiverSuite.TimerName,
      value,
      TimeUnit.NANOSECONDS,
      classOf[ExponentiallyDecayingReservoir],
      classOf[Clock.UserTimeClock]
    ))
    verify(timer, timeout(RpcMetricsReceiverSuite.DefaultRpcEventLoopTimeout)).update(
      ArgumentMatchers.eq(value),
      ArgumentMatchers.eq(TimeUnit.NANOSECONDS)
    )
  }

  test("TimerMessage received with invalid name and update() not called") {
    val value: Long = 12345L
    rpcMetricsReceiverRef.send(TimerMessage(
      RpcMetricsReceiverSuite.MetricNamespace,
      RpcMetricsReceiverSuite.InvalidTimerName,
      value,
      TimeUnit.NANOSECONDS,
      classOf[ExponentiallyDecayingReservoir],
      classOf[Clock.UserTimeClock]
    ))
    verify(timer, timeout(RpcMetricsReceiverSuite.DefaultRpcEventLoopTimeout).times(0)).update(
      any[Long],
      any[TimeUnit]
    )
  }

  test("TimerMessage received with mismatched reservoir and update() not called") {
    val value: Long = 12345L
    rpcMetricsReceiverRef.send(TimerMessage(
      RpcMetricsReceiverSuite.MetricNamespace,
      RpcMetricsReceiverSuite.TimerName,
      value,
      TimeUnit.NANOSECONDS,
      classOf[UniformReservoir],
      classOf[Clock.UserTimeClock]
    ))
    verify(timer, timeout(RpcMetricsReceiverSuite.DefaultRpcEventLoopTimeout).times(0)).update(
      any[Long],
      any[TimeUnit]
    )
  }

  test("TimerMessage received with mismatched clock and update() not called") {
    val value: Long = 12345L
    rpcMetricsReceiverRef.send(TimerMessage(
      RpcMetricsReceiverSuite.MetricNamespace,
      RpcMetricsReceiverSuite.TimerName,
      value,
      TimeUnit.NANOSECONDS,
      classOf[ExponentiallyDecayingReservoir],
      clockClass = clockClazz
    ))
    verify(timer, timeout(RpcMetricsReceiverSuite.DefaultRpcEventLoopTimeout).times(0)).update(
      any[Long],
      any[TimeUnit]
    )
  }

  test("SettableGaugeMessage received with valid name and set() called"){
    val value: Long = 12345L
    rpcMetricsReceiverRef.send(SettableGaugeMessage(
      RpcMetricsReceiverSuite.MetricNamespace,
      RpcMetricsReceiverSuite.SettableGaugeName,
      value
    ))
    verify(settableGauge, timeout(RpcMetricsReceiverSuite.DefaultRpcEventLoopTimeout)).set(ArgumentMatchers.eq(value))
  }

  test("SettableGaugeMessage received with invalid name and set() not called") {
    val value: Long = 12345L
    rpcMetricsReceiverRef.send(SettableGaugeMessage(
      RpcMetricsReceiverSuite.MetricNamespace,
      RpcMetricsReceiverSuite.InvalidSettableGaugeName,
      value
    ))

    verify(settableGauge, timeout(RpcMetricsReceiverSuite.DefaultRpcEventLoopTimeout).times(0)).set(any[Long])
  }

  test("Unsupported message received and processing messages does not stop") {
    rpcMetricsReceiverRef.send(new MetricMessage[Long] {
      override val namespace: String = "test"
      override val metricName: String = "test"
      override val value: Long = 0
    })

    val value: Long = 12345L
    rpcMetricsReceiverRef.send(CounterMessage(
      RpcMetricsReceiverSuite.MetricNamespace,
      RpcMetricsReceiverSuite.CounterName,
      value
    ))

    verify(rpcMetricsReceiver, timeout(RpcMetricsReceiverSuite.DefaultRpcEventLoopTimeout).times(2)).receive
    verify(counter, timeout(RpcMetricsReceiverSuite.DefaultRpcEventLoopTimeout)).inc(ArgumentMatchers.eq(value))
  }

}
