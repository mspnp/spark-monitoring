package com.microsoft.pnp;

public class LogAnalyticsEnvironment {
    public static final String LOG_ANALYTICS_WORKSPACE_ID = "LOG_ANALYTICS_WORKSPACE_ID";
    public static final String LOG_ANALYTICS_WORKSPACE_KEY = "LOG_ANALYTICS_WORKSPACE_KEY";

    public static String getWorkspaceId() {
        return System.getenv(LOG_ANALYTICS_WORKSPACE_ID);
    }

    public static String getWorkspaceKey() {
        return System.getenv(LOG_ANALYTICS_WORKSPACE_KEY);
    }
}
