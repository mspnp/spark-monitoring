package org.apache.spark.listeners

import org.json4s.JsonAST.JValue

trait SparkListenerSink {
  def logEvent(event: Option[JValue])
}
