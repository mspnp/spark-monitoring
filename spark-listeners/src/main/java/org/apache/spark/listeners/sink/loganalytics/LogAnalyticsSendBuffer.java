package org.apache.spark.listeners.sink.loganalytics;

import org.apache.spark.listeners.sink.GenericSendBuffer;
import org.apache.spark.listeners.sink.GenericSendBufferTask;

public class LogAnalyticsSendBuffer extends GenericSendBuffer<String> {
    // We will leave this at 25MB, since the Log Analytics limit is 30MB.
    public static final int DEFAULT_MAX_MESSAGE_SIZE_IN_BYTES = 1024 * 1024 * 25;
    public static final int DEFAULT_BATCH_TIME_IN_MILLISECONDS = 5000;

    private final LogAnalyticsClient client;
    private final String logType;
    private final String timeGeneratedField;

    public LogAnalyticsSendBuffer(
            LogAnalyticsClient client,
            String logType,
            String timeGenerateField) {
        super();
        this.client = client;
        this.logType = logType;
        this.timeGeneratedField = timeGenerateField;
    }

    @Override
    protected GenericSendBufferTask<String> createSendBufferTask() {
        return new LogAnalyticsSendBufferTask(
                this.client,
                this.logType,
                this.timeGeneratedField,
                DEFAULT_MAX_MESSAGE_SIZE_IN_BYTES,
                DEFAULT_BATCH_TIME_IN_MILLISECONDS
        );
    }
}
