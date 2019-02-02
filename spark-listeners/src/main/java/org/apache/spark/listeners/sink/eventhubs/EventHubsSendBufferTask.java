package org.apache.spark.listeners.sink.eventhubs;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventDataBatch;
import com.microsoft.azure.eventhubs.EventHubClient;
import org.apache.spark.listeners.sink.GenericSendBufferTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;

public class EventHubsSendBufferTask extends GenericSendBufferTask<String> {

    private static final Logger logger = LoggerFactory.getLogger(EventHubsSendBufferTask.class);

    private final EventHubClient client;

    public EventHubsSendBufferTask(EventHubClient client,
                                   int maxBatchSizeBytes,
                                   int batchTimeInMilliseconds
    ) {
        super(maxBatchSizeBytes, batchTimeInMilliseconds);
        this.client = client;
    }

    @Override
    protected int calculateDataSize(String data) {
        return data.getBytes().length;
    }

    @Override
    protected void process(List<String> datas) {
        if (datas.isEmpty()) {
            logger.debug("No events to send");
            return;
        }

        logger.debug("EventHubsSendBufferTask.process()");
        try {
            final EventDataBatch events = this.client.createBatch();
            EventData sendEvent;

            for (String data : datas) {
                do {
                    final byte[] bytes = data.getBytes(Charset.defaultCharset());

                    sendEvent = EventData.create(bytes);
                } while (events.tryAdd(sendEvent));

                this.client.sendSync(events);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}

