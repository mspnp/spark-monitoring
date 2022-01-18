package com.microsoft.pnp.client.loganalytics;

import com.microsoft.pnp.client.GenericSendBufferTask;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.List;

public class LogAnalyticsSendBufferTask extends GenericSendBufferTask<String> {

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
            int retry=8;
            int backoff=1;
            while(!client.ready() && retry-- > 0){
                System.err.println("Log Analytics client not ready, waiting: " + backoff + " seconds at time = " + Instant.now());
                TimeUnit.SECONDS.sleep(backoff);
                backoff*=2;
            }
            client.send(sb.toString(), logType, timeGeneratedField);
        } catch (Exception ioe) {
            // We can't do much here since we might be inside a logger
            ioe.printStackTrace();
            Throwable inner = ioe.getCause();
            while(inner != null) {
                System.err.println("Details of nested cause:");
                inner.printStackTrace();
                inner=inner.getCause();
            }
            System.err.println("Buffer causing error on send(body, logType, timestampFieldName):");
            System.err.println("clock time = " + Instant.now());
            System.err.println("logType = " + logType);
            System.err.println("timestampFieldName = " + timeGeneratedField);
            if(System.getenv().getOrDefault("LA_LOGFAILEDBUFFERSEND", "") == "TRUE") {
                System.err.println("body =");
                System.err.println(sb.toString());
            }
        }
    }
}
