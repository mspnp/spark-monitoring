package com.microsoft.pnp.loganalytics;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class LogAnalyticsBufferedClientTest {

    private static final Logger logger = LoggerFactory.getLogger(LogAnalyticsBufferedClientTest.class);

    @Test
    public void batchShouldTimeoutAndCallClient() throws IOException {
        logger.info("batchShouldTimeoutAndCallClient");
        LogAnalyticsClient logAnalyticsClient = mock(LogAnalyticsClient.class);
        LogAnalyticsBufferedClient client = new LogAnalyticsBufferedClient(
                logAnalyticsClient,
                "TestMessageType",
                LogAnalyticsBufferedClient.DEFAULT_MAX_MESSAGE_SIZE_IN_BYTES,
                2000
        );

        client.sendMessage("testing", null);
        try {
            Thread.sleep(3000);
        } catch (InterruptedException ie) {
            logger.info("Thread.sleep() interrupted");
        }

        verify(logAnalyticsClient, times(1))
                .send(anyString(), anyString(), (String)isNull());
    }

    @Test
    public void batchSizeShouldExceedByteLimitAndCallSendTwice() throws IOException {
        logger.info("batchSizeShouldExceedByteLimitAndCallSendTwice");
        LogAnalyticsClient logAnalyticsClient = mock(LogAnalyticsClient.class);
        LogAnalyticsBufferedClient client = new LogAnalyticsBufferedClient(
                logAnalyticsClient,
                "TestMessageType",
                50, 3000
        );

        client.sendMessage("I am a big, long, string of characters.", null);
        client.sendMessage("I am another big, long, string of characters.", null);
        try {
            Thread.sleep(7000);
        } catch (InterruptedException ie) {
            logger.info("Thread.sleep() interrupted");
        }

        verify(logAnalyticsClient, times(2))
                .send(anyString(), anyString(), (String)isNull());
    }
}
