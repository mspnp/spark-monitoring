package org.apache.spark.metrics

import java.util.concurrent.TimeUnit

import com.codahale.metrics.{Clock, ExponentiallyDecayingReservoir, UniformReservoir}
import org.apache.spark.SparkFunSuite
import org.apache.spark.rpc.RpcEndpointRef
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import TestUtils._




object MetricProxiesSuite {
  val MetricNamespace = "testmetrics"
  val CounterName = "testcounter"
  val HistogramName = "testhistogram"
  val MeterName = "testmeter"
  val TimerName = "testtimer"
  val SettableGaugeName = "testsettablegauge"
}

class MetricProxiesSuite extends SparkFunSuite
  with BeforeAndAfterEach {

  import TestImplicits._

  private var rpcMetricsReceiverRef: RpcEndpointRef = null

  val clockClazz = loadOneOf("com.codahale.metrics.jvm.CpuTimeClock", "com.codahale.metrics.Clock$CpuTimeClock").get

  override def beforeEach(): Unit = {
    super.beforeEach
    this.rpcMetricsReceiverRef = mock(classOf[RpcEndpointRef])
  }

  override def afterEach(): Unit = {
    super.afterEach
    this.rpcMetricsReceiverRef = null
  }

  test("CounterProxy calls sendMetric with a CounterMessage for inc()") {
    val proxy = new CounterProxy(
      this.rpcMetricsReceiverRef,
      MetricProxiesSuite.MetricNamespace,
      MetricProxiesSuite.CounterName
    )
    proxy.inc()
    verify(this.rpcMetricsReceiverRef).send(argThat((message: CounterMessage) => message.value === 1))
  }

  test("CounterProxy calls sendMetric with a CounterMessage for inc(Long)") {
    val value = 12345L
    val proxy = new CounterProxy(
      this.rpcMetricsReceiverRef,
      MetricProxiesSuite.MetricNamespace,
      MetricProxiesSuite.CounterName
    )
    proxy.inc(value)
    verify(this.rpcMetricsReceiverRef).send(argThat((message: CounterMessage) => message.value === value))
  }

  test("CounterProxy calls sendMetric with a CounterMessage for dec()") {
    val proxy = new CounterProxy(
      this.rpcMetricsReceiverRef,
      MetricProxiesSuite.MetricNamespace,
      MetricProxiesSuite.CounterName
    )
    proxy.dec()
    verify(this.rpcMetricsReceiverRef).send(argThat((message: CounterMessage) => message.value === -1))
  }

  test("CounterProxy calls sendMetric with a CounterMessage for dec(Long)") {
    val value = 12345L
    val proxy = new CounterProxy(
      this.rpcMetricsReceiverRef,
      MetricProxiesSuite.MetricNamespace,
      MetricProxiesSuite.CounterName
    )
    proxy.dec(value)
    verify(this.rpcMetricsReceiverRef).send(argThat((message: CounterMessage) => message.value === -value))
  }

  test("HistogramProxy calls sendMetric with a HistogramMessage for update(Int)") {
    val value: Integer = 12345
    val proxy = new HistogramProxy(
      this.rpcMetricsReceiverRef,
      MetricProxiesSuite.MetricNamespace,
      MetricProxiesSuite.HistogramName
    )
    proxy.update(value)
    verify(this.rpcMetricsReceiverRef).send(argThat((message: HistogramMessage) => message.value === value.toLong))
  }

  test("HistogramProxy calls sendMetric with a HistogramMessage for update(Long)") {
    val value = 12345L
    val proxy = new HistogramProxy(
      this.rpcMetricsReceiverRef,
      MetricProxiesSuite.MetricNamespace,
      MetricProxiesSuite.HistogramName
    )
    proxy.update(value)
    verify(this.rpcMetricsReceiverRef).send(argThat((message: HistogramMessage) => message.value === value))
  }

  test("HistogramProxy calls sendMetric with a HistogramMessage for update(Long) and non-default reservoir") {
    val value = 12345L
    val proxy = new HistogramProxy(
      this.rpcMetricsReceiverRef,
      MetricProxiesSuite.MetricNamespace,
      MetricProxiesSuite.HistogramName,
      new UniformReservoir)
    proxy.update(value)
    verify(this.rpcMetricsReceiverRef).send(argThat(
      (message: HistogramMessage) => message.value === value && message.reservoirClass === classOf[UniformReservoir]))
  }

  test("MeterProxy calls sendMetric with a MeterMessage for mark()") {
    val proxy = new MeterProxy(
      this.rpcMetricsReceiverRef,
      MetricProxiesSuite.MetricNamespace,
      MetricProxiesSuite.MeterName)
    proxy.mark()
    verify(this.rpcMetricsReceiverRef).send(argThat((message: MeterMessage) => message.value === 1))
  }

  test("MeterProxy calls sendMetric with a MeterMessage for mark(Long)") {
    val value = 12345L
    val proxy = new MeterProxy(
      this.rpcMetricsReceiverRef,
      MetricProxiesSuite.MetricNamespace,
      MetricProxiesSuite.MeterName)
    proxy.mark(value)
    verify(this.rpcMetricsReceiverRef).send(argThat((message: MeterMessage) => message.value === value))
  }

  test("MeterProxy calls sendMetric with a MeterMessage for mark(Long) and non-default clock") {
    val value = 12345L
    val proxy = new MeterProxy(
      this.rpcMetricsReceiverRef,
      MetricProxiesSuite.MetricNamespace,
      MetricProxiesSuite.HistogramName,
      clockClazz.newInstance().asInstanceOf[Clock])
    proxy.mark(value)
    verify(this.rpcMetricsReceiverRef).send(argThat(
      (message: MeterMessage) => message.value === value && message.clockClass === clockClazz))
  }

  test("TimerProxy calls sendMetric with a TimerMessage for update(Long, TimeUnit)") {
    val value = 12345L
    val proxy = new TimerProxy(
      this.rpcMetricsReceiverRef,
      MetricProxiesSuite.MetricNamespace,
      MetricProxiesSuite.TimerName
    )

    proxy.update(value, TimeUnit.SECONDS)
    verify(this.rpcMetricsReceiverRef).send(argThat(
      (message: TimerMessage) => message.value === value && message.timeUnit === TimeUnit.SECONDS))
  }

  test("TimerProxy calls sendMetric with a TimerMessage for time(Callable)") {
    val clock = mock(classOf[Clock])
    // Make our clock return different values the second time so we can verify
    // The internal Meter inside the Timer calls getTick() in it's constructor, so we need to add an extra return
    // Spark3 for some reason, calls it once more, so I need to add a further value to the list
    when(clock.getTick()).thenReturn(1000, 1000, 2000, 3000)
    val proxy = new TimerProxy(
      this.rpcMetricsReceiverRef,
      MetricProxiesSuite.MetricNamespace,
      MetricProxiesSuite.TimerName,
      new ExponentiallyDecayingReservoir,
      clock
    )

    import MetricsProxiesImplicits.callable
    proxy.time(() => Thread.sleep(100))
    verify(this.rpcMetricsReceiverRef).send(argThat(
      (message: TimerMessage) => message.value === 1000 && message.timeUnit === TimeUnit.NANOSECONDS))
  }

  test("TimerProxy calls sendMetric with a TimerMessage for update(Long, TimeUnit) and non-default reservoir and clock") {
    val value = 12345L
    val proxy = new TimerProxy(
      this.rpcMetricsReceiverRef,
      MetricProxiesSuite.MetricNamespace,
      MetricProxiesSuite.TimerName,
      new UniformReservoir,
      clockClazz.newInstance().asInstanceOf[Clock]
    )

    proxy.update(value, TimeUnit.SECONDS)
    verify(this.rpcMetricsReceiverRef).send(argThat(
      (message: TimerMessage) => message.value === value &&
        message.timeUnit === TimeUnit.SECONDS &&
        message.reservoirClass === classOf[UniformReservoir] &&
        message.clockClass === clockClazz))
  }

  test("SettableGaugeProxy calls sendMetric with a SettableGaugeMessage for set(Long)") {
    val value = 12345L
    val proxy = new SettableGaugeProxy[Long](
      this.rpcMetricsReceiverRef,
      MetricProxiesSuite.MetricNamespace,
      MetricProxiesSuite.SettableGaugeName
    )
    proxy.set(value)
    verify(this.rpcMetricsReceiverRef).send(argThat((message: SettableGaugeMessage[Long]) => message.value === value))
  }

  test("SettableGauge getValue returns same value as set(T)") {
    val value = 12345L
    val settableGauge = new SettableGauge[Long] {}
    settableGauge.set(value)
    assert(settableGauge.getValue === value)
  }
}
