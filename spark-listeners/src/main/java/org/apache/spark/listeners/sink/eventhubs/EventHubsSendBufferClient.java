package org.apache.spark.listeners.sink.eventhubs;

import com.microsoft.azure.eventhubs.EventHubClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventHubsSendBufferClient {
    private static final Logger logger = LoggerFactory.getLogger(EventHubsSendBufferClient.class);

    private final EventHubsSendBuffer buffer;

    private final EventHubClient client;

    public EventHubsSendBufferClient(EventHubClient client) {
        this.client = client;
        logger.debug("Creating event hubs buffer");
        this.buffer = new EventHubsSendBuffer(this.client);
    }

    public void sendMessage(String message) {
        // Get buffer for bucketing, in this case, time-generated field
        // since we limit the client to a specific message type.
        // This is because different event types can have differing time fields (i.e. Spark)
        this.buffer.send(message);
    }
}
