package org.apache.spark.listeners.sink.loganalytics;

import org.apache.spark.listeners.sink.GenericSendBufferTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LogAnalyticsSendBufferTask extends GenericSendBufferTask<String> {

    private static final Logger logger = LoggerFactory.getLogger(LogAnalyticsSendBufferTask.class);

    private final LogAnalyticsClient client;
    private final String logType;
    private final String timeGeneratedField;

    public LogAnalyticsSendBufferTask(LogAnalyticsClient client,
                                      String logType,
                                      String timeGenerateField,
                                      int maxBatchSizeBytes,
                                      int batchTimeInMilliseconds
    ) {
        super(maxBatchSizeBytes, batchTimeInMilliseconds);
        this.client = client;
        this.logType = logType;
        this.timeGeneratedField = timeGenerateField;
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

        // Build up Log Analytics "batch" and send.
        // How should we handle failures?  I think there is retry built into the HttpClient,
        // but what if that fails as well?  I suspect we should just log it and move on.

        // We are going to assume that the events are properly formatted
        // JSON strings.  So for now, we are going to just wrap brackets around
        // them.
        StringBuffer sb = new StringBuffer("[");
        for (String data : datas) {
            sb.append(data).append(",");
        }
        sb.deleteCharAt(sb.lastIndexOf(",")).append("]");
        try {
            logger.debug("LogAnalyticsSendBufferTask.process()");
            logger.debug(sb.toString());
            client.send(sb.toString(), logType, timeGeneratedField);
        } catch (Exception ioe) {
            logger.error(ioe.getMessage(), ioe);
        }
    }
}
