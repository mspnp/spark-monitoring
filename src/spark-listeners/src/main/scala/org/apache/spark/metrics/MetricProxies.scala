package org.apache.spark.metrics

import java.util.concurrent.{Callable, TimeUnit}

import com.codahale.metrics._
import org.apache.spark.rpc.RpcEndpointRef

import scala.reflect.ClassTag

object MetricsProxiesImplicits {
  import scala.language.implicitConversions

  implicit def callable[T](f: () => T): Callable[T] =
    new Callable[T]() { def call() = f() }
}

private[metrics] object MetricsProxiesReflectionImplicits {
  private def getField[T: ClassTag](fieldName: String): java.lang.reflect.Field = {
    val field = scala.reflect.classTag[T].runtimeClass.getDeclaredField(fieldName)
    field.setAccessible(true)
    field
  }

  implicit class HistogramReflect(val histogram: Histogram) {
    private lazy val reservoirField = getField[Histogram]("reservoir")
    def getReservoirClass: Class[_ <: Reservoir] = {
      reservoirField.get(histogram).getClass.asInstanceOf[Class[_ <: Reservoir]]
    }
  }

  implicit class MeterReflect(val meter: Meter) {
    private lazy val clockField = getField[Meter]("clock")

    def getClockClass: Class[_ <: Clock] = {
      clockField.get(meter).getClass.asInstanceOf[Class[_ <: Clock]]
    }
  }

  implicit class TimerReflect(val timer: Timer) {
    lazy val clockField = getField[Timer]("clock")
    lazy val histogramField = getField[Timer]("histogram")

    def getClockClass: Class[_ <: Clock] = {
      clockField.get(timer).getClass.asInstanceOf[Class[_ <: Clock]]
    }

    def getHistogram: Histogram = {
      histogramField.get(timer).asInstanceOf[Histogram]
    }
  }
}

sealed trait MetricProxy extends Metric with Serializable {
  protected val metricsEndpoint: RpcEndpointRef

  def sendMetric[T](message: MetricMessage[T]): Unit = {
    metricsEndpoint.send(message)
  }
}

class CounterProxy (
                   override val metricsEndpoint: RpcEndpointRef,
                   val namespace: String,
                   val metricName: String
                   ) extends Counter with MetricProxy {

  override def inc(): Unit = {
    inc(1)
  }

  override def inc(n: Long): Unit = {
    sendMetric(CounterMessage(namespace, metricName, n))
  }

  override def dec(): Unit = {
    dec(1)
  }

  override def dec(n: Long): Unit = {
    inc(-n)
  }
}

class HistogramProxy (
                     override val metricsEndpoint: RpcEndpointRef,
                     val namespace: String,
                     val metricName: String,
                     val reservoir: Reservoir = new ExponentiallyDecayingReservoir
                     ) extends Histogram(reservoir) with MetricProxy {
  override def update(value: Long): Unit = {
    sendMetric(HistogramMessage(namespace, metricName, value, reservoir.getClass))
  }
}

class MeterProxy (
                 override val metricsEndpoint: RpcEndpointRef,
                 val namespace: String,
                 val metricName: String,
                 val clock: Clock = Clock.defaultClock
                 ) extends Meter(clock) with MetricProxy {
  override def mark(n: Long): Unit = {
    sendMetric(MeterMessage(namespace, metricName, n, clock.getClass))
  }
}

class TimerProxy (
                 override val metricsEndpoint: RpcEndpointRef,
                 val namespace: String,
                 val metricName: String,
                 val reservoir: Reservoir = new ExponentiallyDecayingReservoir,
                 val clock: Clock = Clock.defaultClock
                 ) extends Timer(reservoir, clock) with MetricProxy {

  override def update(duration: Long, unit: TimeUnit): Unit = {
    sendMetric(TimerMessage(
      namespace,
      metricName,
      duration,
      unit,
      reservoir.getClass,
      clock.getClass
    ))
  }

  // We need to override the time(Callable<T>) method because it bypasses the update(Long, TimeUnit)
  // method.
  override def time[T](event: Callable[T]): T = {
    val context = this.time()
    try {
      event.call()
    } finally {
      context.close()
    }
  }
}

trait SettableGauge[T] extends Gauge[T] {
  protected var value: T = _
  def set(value: T): Unit = {
    this.value = value
  }

  override def getValue: T = {
    this.value
  }
}

class SettableGaugeProxy[T](
                             override val metricsEndpoint: RpcEndpointRef,
                             val namespace: String,
                             val metricName: String
                           ) extends SettableGauge[T] with MetricProxy {
  override def set(value: T): Unit = {
    // We don't really need to set this, but we will, just in case
    sendMetric(SettableGaugeMessage[T](namespace, metricName, value))
  }
}
