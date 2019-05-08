package com.microsoft.pnp.logging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.Layout;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

public class JSONLayout extends Layout {

    public static final String TIMESTAMP_FIELD_NAME = "timestamp";
    private boolean locationInfo;
    private String jsonConfiguration;
    private ObjectMapper objectMapper = new ObjectMapper();

    public JSONLayout() {
        this(false);
    }

    /**
     * Creates a layout that optionally inserts location information into log messages.
     *
     * @param locationInfo whether or not to include location information in the log messages.
     */
    public JSONLayout(boolean locationInfo) {
        this.locationInfo = locationInfo;
    }

    public String format(LoggingEvent loggingEvent) {
        String threadName = loggingEvent.getThreadName();
        long timestamp = loggingEvent.getTimeStamp();
        Map mdc = loggingEvent.getProperties();
        ObjectNode event = this.objectMapper.createObjectNode();

        event.put(TIMESTAMP_FIELD_NAME, Instant.ofEpochMilli(timestamp).toString());

        event.put("message", loggingEvent.getRenderedMessage());

        if (loggingEvent.getThrowableInformation() != null) {
            ObjectNode exceptionNode = objectMapper.createObjectNode();
            final ThrowableInformation throwableInformation = loggingEvent.getThrowableInformation();
            if (throwableInformation.getThrowable().getClass().getCanonicalName() != null) {
                exceptionNode.put("exception_class", throwableInformation.getThrowable().getClass().getCanonicalName());
            }
            if (throwableInformation.getThrowable().getMessage() != null) {
                exceptionNode.put("exception_message", throwableInformation.getThrowable().getMessage());
            }
            if (throwableInformation.getThrowableStrRep() != null) {
                String stackTrace = String.join("\n", throwableInformation.getThrowableStrRep());
                exceptionNode.put("stacktrace", stackTrace);
            }
            event.replace("exception", exceptionNode);
        }

        if (locationInfo) {
            LocationInfo info = loggingEvent.getLocationInformation();
            event.put("file", info.getFileName());
            event.put("line_number", info.getLineNumber());
            event.put("class", info.getClassName());
            event.put("method", info.getMethodName());
        }

        event.put("logger_name", loggingEvent.getLoggerName());
        event.put("level", loggingEvent.getLevel().toString());
        event.put("thread_name", threadName);

        // We are going to change this to promote all MDC properties to top-level properties.
        // If there is a name collision, we will just log a warning to LogLog.
        // Because of this, we need to process MDC as the last set of properties.
        for (Map.Entry property : (Set<Map.Entry>)mdc.entrySet()) {
            String key = (String)property.getKey();
            if (event.has(key)) {
                // The field in MDC has a name conflict with the existing fields.
                LogLog.warn(String.format("MDC field '%s' already exists in log4j event", key));
            } else {
                event.put(key, (String)property.getValue());
            }
        }

        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception ex) {
            LogLog.warn("Error serializing event", ex);
            return null;
        }
    }

    public boolean ignoresThrowable() {
        return false;
    }

    /**
     * Query whether log messages include location information.
     *
     * @return true if location information is included in log messages, false otherwise.
     */
    public boolean getLocationInfo() {
        return this.locationInfo;
    }

    public void setLocationInfo(boolean locationInfo) {
        this.locationInfo = locationInfo;
    }

    public String getJsonConfiguration() {
        return this.jsonConfiguration;
    }

    public void setJsonConfiguration(String jsonConfiguration) {
        try {
            Class clazz = Class.forName(jsonConfiguration);
            JSONConfiguration configuration = (JSONConfiguration)clazz.newInstance();
            configuration.configure(this.objectMapper);
        } catch (ClassNotFoundException cnfe) {
            LogLog.warn(
                    String.format("Could not find JSON Configuration class: %s", jsonConfiguration),
                    cnfe);
        } catch (InstantiationException | IllegalAccessException ie) {
            LogLog.warn(
                    String.format("Error creating instance of JSON Configuration class: %s", jsonConfiguration),
                    ie);
        } catch (Exception ex) {
            LogLog.warn("Unexpected error setting JSON Configuration", ex);
        }
    }

    public void activateOptions() {
    }
}