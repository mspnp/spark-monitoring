package org.apache.spark.listeners.sink.loganalytics

import com.microsoft.pnp.LogAnalyticsEnvironment
import org.apache.spark.SparkConf
import org.apache.spark.com.microsoft.pnp.LogAnalyticsConfiguration

private[spark] object LogAnalyticsListenerSinkConfiguration {
  private val CONFIG_PREFIX = "spark.logAnalytics"

  private[spark] val WORKSPACE_ID = CONFIG_PREFIX + ".workspaceId"

  // We'll name this secret so Spark will redact it.
  private[spark] val SECRET = CONFIG_PREFIX + ".secret"

  private[spark] val LOG_TYPE = CONFIG_PREFIX + ".logType"

  private[spark] val DEFAULT_LOG_TYPE = "SparkListenerEvent"

  private[spark] val TIMESTAMP_FIELD_NAME = CONFIG_PREFIX + ".timestampFieldName"

  //private[spark] val ENV_LOG_ANALYTICS_WORKSPACEID = "LOG_ANALYTICS_WORKSPACEID"

  ///private[spark] val ENV_LOG_ANALYTICS_SECRET = "LOG_ANALYTICS_SECRET"
}

private[spark] class LogAnalyticsListenerSinkConfiguration(sparkConf: SparkConf)
  extends LogAnalyticsConfiguration {

  import LogAnalyticsListenerSinkConfiguration._

  override def getWorkspaceId: Option[String] = {
    // Match spark priority order
    //sparkConf.getOption(WORKSPACE_ID).orElse(sys.env.get(ENV_LOG_ANALYTICS_WORKSPACEID))
    sparkConf.getOption(WORKSPACE_ID).orElse(Option(LogAnalyticsEnvironment.getWorkspaceId))
  }

  override def getSecret: Option[String] = {
    // Match spark priority order
    //sparkConf.getOption(SECRET).orElse(sys.env.get(ENV_LOG_ANALYTICS_SECRET))
    sparkConf.getOption(SECRET).orElse(Option(LogAnalyticsEnvironment.getWorkspaceKey))
  }

  override def getLogType: String = sparkConf.get(LOG_TYPE, DEFAULT_LOG_TYPE)

  override def getTimestampFieldName: Option[String] = sparkConf.getOption(TIMESTAMP_FIELD_NAME)
}
