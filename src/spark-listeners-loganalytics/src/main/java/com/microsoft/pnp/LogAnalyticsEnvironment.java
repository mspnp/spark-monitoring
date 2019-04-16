package com.microsoft.pnp;
import org.apache.commons.lang3.StringUtils;
public class LogAnalyticsEnvironment {
    public static final String LOG_ANALYTICS_WORKSPACE_ID = "LOG_ANALYTICS_WORKSPACE_ID";
    public static final String LOG_ANALYTICS_WORKSPACE_KEY = "LOG_ANALYTICS_WORKSPACE_KEY";
    public static final String SOURCE_CLUSTER_ID = "SOURCE_CLUSTER_ID";
    public static final String IS_SPARK_DRIVER_NODE = "IS_SPARK_DRIVER_NODE";
    public static final String LOG4J_TABLE_NAME = "LOG4J_TABLE_NAME";
    private static final String DEFAULT_LOG_TYPE = "SparkLoggingEvent";

    public static String getWorkspaceId() {
        return System.getenv(LOG_ANALYTICS_WORKSPACE_ID);
    }
    public static String getWorkspaceKey() {
        return System.getenv(LOG_ANALYTICS_WORKSPACE_KEY);
    }

    // Get cluster ID and whether Driver node or not from environment variables in init script
    public static String getClusterId() {
        return System.getenv(SOURCE_CLUSTER_ID);
    }
    public static String isDriverNode() { return System.getenv(IS_SPARK_DRIVER_NODE); }
    public static String getLog4jTableName() {
        String log4jTableName =  System.getenv(LOG4J_TABLE_NAME);
        if (!StringUtils.isEmpty(log4jTableName))
             return System.getenv(LOG4J_TABLE_NAME);
        else
             return DEFAULT_LOG_TYPE;
    }
  }
