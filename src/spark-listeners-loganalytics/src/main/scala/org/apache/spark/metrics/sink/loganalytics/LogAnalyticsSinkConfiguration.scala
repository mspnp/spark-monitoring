package org.apache.spark.metrics.sink.loganalytics

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.microsoft.pnp.LogAnalyticsEnvironment
import org.apache.spark.com.microsoft.pnp.LogAnalyticsConfiguration

private[spark] object LogAnalyticsSinkConfiguration {
  private[spark] val LOGANALYTICS_KEY_WORKSPACEID = "workspaceId"
  private[spark] val LOGANALYTICS_KEY_SECRET = "secret"
  private[spark] val LOGANALYTICS_KEY_LOGTYPE = "logType"
  private[spark] val LOGANALYTICS_KEY_TIMESTAMPFIELD = "timestampField"
  private[spark] val LOGANALYTICS_KEY_PERIOD = "period"
  private[spark] val LOGANALYTICS_KEY_UNIT = "unit"

  private[spark] val LOGANALYTICS_DEFAULT_LOGTYPE = "SparkMetrics"
  private[spark] val LOGANALYTICS_DEFAULT_PERIOD = "10"
  private[spark] val LOGANALYTICS_DEFAULT_UNIT = "SECONDS"
}

private[spark] class LogAnalyticsSinkConfiguration(properties: Properties)
  extends LogAnalyticsConfiguration {

  import LogAnalyticsSinkConfiguration._

  override def getWorkspaceId: Option[String] = {
    Option(properties.getProperty(LOGANALYTICS_KEY_WORKSPACEID, LogAnalyticsEnvironment.getWorkspaceId))
  }

  override def getSecret: Option[String] = {
    Option(properties.getProperty(LOGANALYTICS_KEY_SECRET, LogAnalyticsEnvironment.getWorkspaceKey))
  }

  override protected def getLogType: String =
    properties.getProperty(LOGANALYTICS_KEY_LOGTYPE, LOGANALYTICS_DEFAULT_LOGTYPE)

  override protected def getTimestampFieldName: Option[String] =
    Option(properties.getProperty(LOGANALYTICS_KEY_TIMESTAMPFIELD, null))

  val pollPeriod: Int = {
    val value = properties.getProperty(LOGANALYTICS_KEY_PERIOD, LOGANALYTICS_DEFAULT_PERIOD).toInt
    logInfo(s"Setting polling period to $value")
    value
  }

  val pollUnit: TimeUnit = {
    val value = TimeUnit.valueOf(
      properties.getProperty(LOGANALYTICS_KEY_UNIT, LOGANALYTICS_DEFAULT_UNIT).toUpperCase)
    logInfo(s"Setting polling unit to $value")
    value
  }
}
