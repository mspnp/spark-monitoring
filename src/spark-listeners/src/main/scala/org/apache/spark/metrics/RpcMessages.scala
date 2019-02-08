package org.apache.spark.metrics

import java.util.concurrent.TimeUnit

import com.codahale.metrics.{Clock, Reservoir}

trait MetricMessage[T] {
  val namespace: String
  val metricName: String
  val value: T
}

private[metrics] case class CounterMessage(
                                          override val namespace: String,
                                          override val metricName: String,
                                          override val value: Long
                                          ) extends MetricMessage[Long]

private[metrics] case class SettableGaugeMessage[T](
                                                                   override val namespace: String,
                                                                   override val metricName: String,
                                                                   override val value: T
                                                                   ) extends MetricMessage[T]

import scala.language.existentials

private[metrics] case class HistogramMessage(
                                            override val namespace: String,
                                            override val metricName: String,
                                            override val value: Long,
                                            reservoirClass: Class[_ <: Reservoir]
                                            ) extends MetricMessage[Long]

private[metrics] case class MeterMessage(
                                        override val namespace: String,
                                        override val metricName: String,
                                        override val value: Long,
                                        clockClass: Class[_ <: Clock]
                                        ) extends MetricMessage[Long]

private[metrics] case class TimerMessage(
                                        override val namespace: String,
                                        override val metricName: String,
                                        override val value: Long,
                                        timeUnit: TimeUnit,
                                        reservoirClass: Class[_ <: Reservoir],
                                        clockClass: Class[_ <: Clock]
                                        ) extends MetricMessage[Long]
