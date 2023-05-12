package com.microsoft.pnp.loganalytics

import com.microsoft.pnp.LogAnalyticsEnvironment

import java.util.Properties
import java.util.concurrent.TimeUnit

private[loganalytics] object LogAnalyticsSinkConfiguration {
  private[loganalytics] val LOGANALYTICS_KEY_WORKSPACEID = "workspaceId"
  private[loganalytics] val LOGANALYTICS_KEY_SECRET = "secret"
  private[loganalytics] val LOGANALYTICS_KEY_LOGTYPE = "logType"
  private[loganalytics] val LOGANALYTICS_KEY_TIMESTAMPFIELD = "timestampField"
  private[loganalytics] val LOGANALYTICS_KEY_PERIOD = "period"
  private[loganalytics] val LOGANALYTICS_KEY_UNIT = "unit"

  private[loganalytics] val LOGANALYTICS_DEFAULT_LOGTYPE = "SparkMetrics"
  private[loganalytics] val LOGANALYTICS_DEFAULT_PERIOD = "10"
  private[loganalytics] val LOGANALYTICS_DEFAULT_UNIT = "SECONDS"
}

class LogAnalyticsSinkConfiguration(properties: Properties)
  extends LogAnalyticsConfiguration {

  import LogAnalyticsSinkConfiguration._

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
}
