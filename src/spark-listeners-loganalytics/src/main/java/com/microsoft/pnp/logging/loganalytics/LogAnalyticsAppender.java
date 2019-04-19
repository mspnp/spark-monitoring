package com.microsoft.pnp.logging.loganalytics;

import com.microsoft.pnp.LogAnalyticsEnvironment;
import com.microsoft.pnp.client.loganalytics.LogAnalyticsClient;
import com.microsoft.pnp.client.loganalytics.LogAnalyticsSendBufferClient;
import com.microsoft.pnp.logging.JSONLayout;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.spark.sql.SparkSession;
import scala.runtime.AbstractFunction0;

import static com.microsoft.pnp.logging.JSONLayout.TIMESTAMP_FIELD_NAME;

public class LogAnalyticsAppender extends AppenderSkeleton {
    private static final Filter ORG_APACHE_HTTP_FILTER = new Filter() {
        @Override
        public int decide(LoggingEvent loggingEvent) {
            if (loggingEvent.getLoggerName().startsWith("org.apache.http")) {
                return Filter.DENY;
            }

            return Filter.ACCEPT;
        }
    };

    private static final String DEFAULT_LOG_TYPE = "SparkLoggingEvent";
    // We will default to environment so the properties file can override
    private String workspaceId = LogAnalyticsEnvironment.getWorkspaceId();
    private String secret = LogAnalyticsEnvironment.getWorkspaceKey();
    private String logType = DEFAULT_LOG_TYPE;
    private LogAnalyticsSendBufferClient client;

    private String applicationId;
    private String clusterId;
    private String executorId;

    public LogAnalyticsAppender() {
        this.addFilter(ORG_APACHE_HTTP_FILTER);
        // Add a default layout so we can simplify config
        this.setLayout(new JSONLayout());
    }

    @Override
    public void activateOptions() {
        this.client = new LogAnalyticsSendBufferClient(
                new LogAnalyticsClient(this.workspaceId, this.secret),
                this.logType
        );

        this.executorId = System.getProperty(
                "local.spark.executor.id",
                null);
        this.clusterId = System.getProperty(
                "local.spark.databricks.clusterUsageTags.clusterId",
                null
        );

        // If we are on the executor, we can get the application id
        if (this.executorId != "driver") {
            this.applicationId = System.getProperty(
                    "local.spark.application.id",
                    null
            );
        }
    }

    @Override
    protected void append(LoggingEvent loggingEvent) {
        try {
            // If we are on the driver, we may or may not have an application id yet
            String localApplicationId = null;

            if ("driver".equals(this.executorId)) {
                // We need to check each time we log since on the driver it's a different lifecycle
                // See if we have an active session.  If not, see if we have a default session.
                SparkSession sparkSession = SparkSession.getActiveSession().getOrElse(
                        new AbstractFunction0<SparkSession>() {
                            @Override
                            public SparkSession apply() {
                                return SparkSession.getDefaultSession().getOrElse(new AbstractFunction0<SparkSession>() {
                                    @Override
                                    public SparkSession apply() {
                                        return null;
                                    }
                                });
                            }
                        });
                if (sparkSession != null) {
                    localApplicationId = sparkSession.sparkContext().applicationId();
                }
            } else {
                // If we are not on the driver, the applicationId should have been given to us in
                // system properties, so this should be okay
                localApplicationId = this.applicationId;
            }

            // Add extra stuff
            if (localApplicationId != null) {
                loggingEvent.setProperty("applicationId", localApplicationId);
            }
            if (this.executorId != null) {
                loggingEvent.setProperty("executorId", this.executorId);
            }
            if (this.clusterId != null) {
                loggingEvent.setProperty("clusterId", this.clusterId);
            }
            String json = this.getLayout().format(loggingEvent);
            this.client.sendMessage(json, TIMESTAMP_FIELD_NAME);
        } catch (Exception ex) {
            LogLog.error("Error sending logging event to Log Analytics", ex);
        }
    }

    @Override
    public boolean requiresLayout() {
        // We will set this to false so we can simplify our config
        // If no layout is provided, we will get the default.
        return false;
    }

    @Override
    public void close() {
        this.client.close();
    }

    @Override
    public void setLayout(Layout layout) {
        // This will allow us to configure the layout from properties to add custom JSON stuff.
        if (!(layout instanceof JSONLayout)) {
            throw new UnsupportedOperationException("layout must be an instance of JSONLayout");
        }

        super.setLayout(layout);
    }

    @Override
    public void clearFilters() {
        super.clearFilters();
        // We need to make sure to add the filter back so we don't get stuck in a loop
        this.addFilter(ORG_APACHE_HTTP_FILTER);
    }

    public String getWorkspaceId() {
        return this.workspaceId;
    }

    public void setWorkspaceId(String workspaceId) {
        this.workspaceId = workspaceId;
    }

    public String getSecret() {
        return this.secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    public String getLogType() {
        return this.logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }
}
