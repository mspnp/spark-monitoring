package org.apache.spark.metrics

import java.util.concurrent.TimeUnit

import com.codahale.metrics._
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcEndpoint, RpcEnv}

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.reflect.ClassTag

class RpcMetricsReceiver(val sparkEnv: SparkEnv,
                         val sources: Seq[org.apache.spark.metrics.source.Source])
  extends RpcEndpoint with Logging {
  override val rpcEnv: RpcEnv = sparkEnv.rpcEnv

  private val metricsSources: Map[String, Map[String, Metric]] =
    sources.map(
      source => source.sourceName -> source.metricRegistry.getMetrics.asScala.toMap
    )
    .toMap

  import MetricsProxiesReflectionImplicits._

  override def receive: PartialFunction[Any, Unit] = {
    case CounterMessage(namespace, metricName, value) => {
      getMetric[Counter](namespace, metricName) match {
        case Some(counter) => {
          logDebug(s"inc(${value}) called on counter '${metricName}', in namespace '${namespace}'")
          counter.inc(value)
        }
        case None => logWarning(s"Counter '${metricName}' not found")
      }
    }
    case HistogramMessage(namespace, metricName, value, reservoirClass) => {
      getMetric[Histogram](namespace, metricName) match {
        case Some(histogram) => {
          val histogramReservoirClass = histogram.getReservoirClass
          if (histogramReservoirClass != reservoirClass) {
            logWarning(s"Proxy reservoir class ${reservoirClass.getCanonicalName} does not match driver reservoir class ${histogramReservoirClass.getCanonicalName}")
          } else {
            histogram.update(value)
          }
        }
        case None => logWarning(s"Histogram '${metricName}' not found")
      }
    }
    case MeterMessage(namespace, metricName, value, clockClass) => {
      getMetric[Meter](namespace, metricName) match {
        case Some(meter) => {
          val meterClockClass = meter.getClockClass
          if (meterClockClass != clockClass) {
            logWarning(s"Proxy meter class ${clockClass.getCanonicalName} does not match driver clock class ${meterClockClass.getCanonicalName}")
          } else {
            meter.mark(value)
          }
        }
        case None => logWarning(s"Meter '${metricName}' not found")
      }
    }

    case TimerMessage(namespace, metricName, value, unit, reservoirClass, clockClass) => {
      getMetric[Timer](namespace, metricName) match {
        case Some(timer) => {
          val timerClockClass = timer.getClockClass
          val timerReservoirClass = timer.getHistogram.getReservoirClass
          if (timerClockClass != clockClass) {
            logWarning(s"Proxy clock class ${clockClass.getCanonicalName} does not match driver clock class ${timerClockClass.getCanonicalName}")
          } else if (timerReservoirClass != reservoirClass) {
            logWarning(s"Proxy reservoir class ${reservoirClass.getCanonicalName} does not match driver reservoir class ${timerReservoirClass.getCanonicalName}")
          } else {
            // Everything looks good
            timer.update(value, unit)
          }
        }
        case None => logWarning(s"Timer '${metricName}' not found")
      }
    }
    case SettableGaugeMessage(namespace, metricName, value) => {
      getMetric[SettableGauge[Any]](namespace, metricName) match {
        case Some(gauge) => gauge.set(value)
        case None => logWarning(s"SettableGauge '${metricName}' not found")
      }
    }
    case message: Any => logWarning(s"Unsupported message type: $message")
  }

  private[metrics] def getMetric[T <: Metric](namespace: String, metricName: String)(implicit tag: ClassTag[T]): Option[T] = {
    metricsSources.get(namespace) match {
      case Some(metrics) => {
        metrics.get(metricName) match {
          case Some(metric) => {
            if (tag.runtimeClass.isInstance(metric)) Some(metric.asInstanceOf[T]) else None
          }
          case _ => None
        }
      }
      case _ => None
    }
  }
}

object RpcMetricsReceiver {
  val DefaultTimeUnit = TimeUnit.NANOSECONDS
  val DefaultEndpointName = "MetricsReceiver"
}