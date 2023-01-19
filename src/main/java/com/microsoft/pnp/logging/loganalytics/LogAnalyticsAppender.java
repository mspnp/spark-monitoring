package com.microsoft.pnp.logging.loganalytics;

import com.microsoft.pnp.LogAnalyticsEnvironment;
import com.microsoft.pnp.client.loganalytics.LogAnalyticsClient;
import com.microsoft.pnp.client.loganalytics.LogAnalyticsSendBufferClient;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

import java.io.Serializable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


@Plugin(name = "LogAnalyticsAppender",
        category = "Core",
        elementType = "appender",
        printObject = true)
public class LogAnalyticsAppender extends AbstractAppender {

    private static final String DEFAULT_LOG_TYPE = "SparkLoggingEvent";
    // We will default to environment so the properties file can override
    private final String workspaceId = LogAnalyticsEnvironment.getWorkspaceId();
    private final String secret = LogAnalyticsEnvironment.getWorkspaceKey();
    private LogAnalyticsSendBufferClient client;

    LogAnalyticsAppender(String name,
                         Layout<? extends Serializable> layout,
                         Filter filter) {
        super(name, filter, layout, false, Property.EMPTY_ARRAY);

    }


    @PluginFactory
    public static LogAnalyticsAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Filter") Filter filter) {
        return new LogAnalyticsAppender(name, layout, filter);
    }

    @Override
    public void start() {
        super.start();
        this.client = new LogAnalyticsSendBufferClient(
                new LogAnalyticsClient(this.workspaceId, this.secret),
                DEFAULT_LOG_TYPE
        );
    }

    @Override
    public void stop() {
        this.client.close();
        super.stop();
    }

    @Override
    protected boolean stop(Future<?> future) {
        this.client.close();
        return super.stop(future);
    }

    @Override
    public boolean stop(long timeout, TimeUnit timeUnit) {
        this.client.close();
        return super.stop(timeout, timeUnit);
    }

    @Override
    public void append(LogEvent logEvent) {
        if (!this.isStarted()) {
            LOGGER.warn("Appender not initialized yet ...");
            return;
        }
        try {
            String message = new String(this.getLayout().toByteArray(logEvent));
            this.client.sendMessage(message, "timestamp");
        } catch (Exception e) {
            LOGGER.error(e);
        }
    }
}