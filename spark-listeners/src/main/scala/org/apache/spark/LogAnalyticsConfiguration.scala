package org.apache.spark

import org.apache.spark.internal.Logging

private[spark] trait LogAnalyticsConfiguration extends Logging {
  protected def getWorkspaceId: Option[String]

  protected def getSecret: Option[String]

  protected def getLogType: String

  protected def getTimestampFieldName: Option[String]


  val workspaceId: String = {
    val value = getWorkspaceId
    require(value.isDefined, "A Log Analytics Workspace ID is required")
    logInfo(s"Setting workspaceId to ${value.get}")
    value.get

  }

  val secret: String = {
    val value = getSecret
    require(value.isDefined, "A Log Analytics Workspace Key is required")
    value.get
  }


  val logType: String = {
    val value = getLogType
    logInfo(s"Setting logType to $value")
    value
  }

  val timestampFieldName: String = {
    val value = getTimestampFieldName
    logInfo(s"Setting timestampNameField to $value")
    value.orNull
  }
}


private[spark] object LogAnalyticsListenerConfiguration {
  private val CONFIG_PREFIX = "spark.logAnalytics"

  private[spark] val WORKSPACE_ID = CONFIG_PREFIX + ".workspaceId"

  // We'll name this secret so Spark will redact it.
  private[spark] val SECRET = CONFIG_PREFIX + ".secret"

  private[spark] val LOG_TYPE = CONFIG_PREFIX + ".logType"

  private[spark] val DEFAULT_LOG_TYPE = "SparkListenerEvent"

  private[spark] val TIMESTAMP_FIELD_NAME = CONFIG_PREFIX + ".timestampFieldName"

  private[spark] val LOG_BLOCK_UPDATES = CONFIG_PREFIX + ".logBlockUpdates"

  private[spark] val DEFAULT_LOG_BLOCK_UPDATES = false

  private[spark] val ENV_LOG_ANALYTICS_WORKSPACEID = "LOG_ANALYTICS_WORKSPACEID"

  private[spark] val ENV_LOG_ANALYTICS_SECRET = "LOG_ANALYTICS_SECRET"
}

private[spark] class LogAnalyticsListenerConfiguration(sparkConf: SparkConf)
  extends LogAnalyticsConfiguration {

  import LogAnalyticsListenerConfiguration._

  override def getWorkspaceId: Option[String] = {

    if (sys.env.contains(ENV_LOG_ANALYTICS_WORKSPACEID)) {
      sys.env.get(ENV_LOG_ANALYTICS_WORKSPACEID)
    }
    else {
      sparkConf.getOption(WORKSPACE_ID)
    }

  }

  override def getSecret: Option[String] = {

    if (sys.env.contains(ENV_LOG_ANALYTICS_SECRET)) {
      sys.env.get(ENV_LOG_ANALYTICS_SECRET)
    }
    else {
      sparkConf.getOption(SECRET)
    }

  }

  override def getLogType: String = sparkConf.get(LOG_TYPE, DEFAULT_LOG_TYPE)

  override def getTimestampFieldName: Option[String] = sparkConf.getOption(TIMESTAMP_FIELD_NAME)

  def logBlockUpdates: Boolean = {
    val value = sparkConf.getBoolean(LOG_BLOCK_UPDATES, DEFAULT_LOG_BLOCK_UPDATES)
    logInfo(s"Setting logBlockUpdates to $value")
    value
  }
}
