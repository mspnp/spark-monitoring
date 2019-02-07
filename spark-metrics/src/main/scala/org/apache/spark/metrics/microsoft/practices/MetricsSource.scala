package org.apache.spark.metrics.microsoft.practices

import com.codahale.metrics.MetricRegistry

case class MetricsSource(
                        override val sourceName: String,
                        override val metricRegistry: MetricRegistry
                        ) extends org.apache.spark.metrics.source.Source
