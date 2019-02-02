package org.apache.spark.listeners.sink.eventhubs;

import com.microsoft.azure.eventhubs.EventHubClient;
import org.apache.spark.listeners.sink.GenericSendBuffer;
import org.apache.spark.listeners.sink.GenericSendBufferTask;

public class EventHubsSendBuffer extends GenericSendBuffer<String> {
    // We will leave this at 25MB, since the Log Analytics limit is 30MB.
    public static final int DEFAULT_MAX_MESSAGE_SIZE_IN_BYTES = 1024 * 1024 * 25;
    public static final int DEFAULT_BATCH_TIME_IN_MILLISECONDS = 5000;

    private final EventHubClient client;

    public EventHubsSendBuffer(
            EventHubClient client) {
        super();
        this.client = client;
    }

    @Override
    protected GenericSendBufferTask<String> createSendBufferTask() {
        return new EventHubsSendBufferTask(
                this.client,
                DEFAULT_MAX_MESSAGE_SIZE_IN_BYTES,
                DEFAULT_BATCH_TIME_IN_MILLISECONDS
        );
    }
}