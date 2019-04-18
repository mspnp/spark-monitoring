package com.microsoft.pnp.client.loganalytics;

import com.microsoft.pnp.client.GenericSendBuffer;

import java.util.LinkedHashMap;

public class LogAnalyticsSendBufferClient implements AutoCloseable {
    private final LinkedHashMap<String, LogAnalyticsSendBuffer> buffers = new LinkedHashMap<>();

    private final LogAnalyticsClient client;
    private final String logType;
    private final int maxMessageSizeInBytes;
    private final int batchTimeInMilliseconds;

    // We will leave this at 25MB, since the Log Analytics limit is 30MB.
    public static final int DEFAULT_MAX_MESSAGE_SIZE_IN_BYTES = 1024 * 1024 * 25;
    public static final int DEFAULT_BATCH_TIME_IN_MILLISECONDS = 5000;

    public LogAnalyticsSendBufferClient(LogAnalyticsClient client, String messageType) {
        this(
                client,
                messageType,
                DEFAULT_MAX_MESSAGE_SIZE_IN_BYTES,
                DEFAULT_BATCH_TIME_IN_MILLISECONDS
        );
    }

    public LogAnalyticsSendBufferClient(LogAnalyticsClient client,
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
        LogAnalyticsSendBuffer buffer = this.getBuffer(timeGeneratedField);
        buffer.send(message);
    }

    private synchronized LogAnalyticsSendBuffer getBuffer(String timeGeneratedField) {
        LogAnalyticsSendBuffer buffer = this.buffers.get(timeGeneratedField);
        if (null == buffer) {
            buffer = new LogAnalyticsSendBuffer(
                    this.client,
                    this.logType,
                    timeGeneratedField);
            this.buffers.put(timeGeneratedField, buffer);
        }

        return buffer;
    }

    @Override
    public void close() {
        this.buffers.values().forEach(GenericSendBuffer::close);
    }
}
