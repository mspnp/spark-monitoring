package org.apache.spark.listeners.microsoft.pnp.loganalytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;

public class LogAnalyticsBufferedClient {
    private static final Logger logger = LoggerFactory.getLogger(LogAnalyticsBufferedClient.class);

    private final LinkedHashMap<String, SendBuffer> buffers = new LinkedHashMap<>();

    private final LogAnalyticsClient client;
    private final String logType;
    private final int maxMessageSizeInBytes;
    private final int batchTimeInMilliseconds;

    // We will leave this at 25MB, since the Log Analytics limit is 30MB.
    public static final int DEFAULT_MAX_MESSAGE_SIZE_IN_BYTES = 1024 * 1024 * 25;
    public static final int DEFAULT_BATCH_TIME_IN_MILLISECONDS = 5000;

    public LogAnalyticsBufferedClient(LogAnalyticsClient client, String messageType) {
        this(
                client,
                messageType,
                DEFAULT_MAX_MESSAGE_SIZE_IN_BYTES,
                DEFAULT_BATCH_TIME_IN_MILLISECONDS
        );
    }

    public LogAnalyticsBufferedClient(LogAnalyticsClient client,
                                      String logType,
                                      int maxMessageSizeInBytes,
                                      int batchTimeInMilliseconds) {
        this.client = client;
        this.logType = logType;
        this.maxMessageSizeInBytes = maxMessageSizeInBytes;
        this.batchTimeInMilliseconds = batchTimeInMilliseconds;
    }

    public void sendMessage(String message, String timeGeneratedField) {
        // Get buffer for bucketing, in this case, time-generated field
        // since we limit the client to a specific message type.
        // This is because different event types can have differing time fields (i.e. Spark)
        SendBuffer buffer = this.getBuffer(timeGeneratedField);
        buffer.sendMessage(message);
    }

    private synchronized SendBuffer getBuffer(String timeGeneratedField) {
        logger.debug("Getting buffer for key: " + (timeGeneratedField == null ? "null" : timeGeneratedField));
        SendBuffer buffer = this.buffers.get(timeGeneratedField);
        if (null == buffer) {
            logger.debug("Buffer was null....creating");
            buffer = new SendBuffer(this.client, this.logType, timeGeneratedField, this.maxMessageSizeInBytes,
                    this.batchTimeInMilliseconds);
            this.buffers.put(timeGeneratedField, buffer);
        }

        return buffer;
    }
}
