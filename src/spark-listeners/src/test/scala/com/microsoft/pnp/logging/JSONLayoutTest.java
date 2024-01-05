package com.microsoft.pnp.logging;

import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JSONLayoutTest {
    private JSONLayout jsonLayout;

    @Before
    public void beforeEach() {
        jsonLayout = new JSONLayout();
    }

    @Test
    public void shouldSerializeWithoutTrainingNewline() {
        jsonLayout.setOneLogEventPerLine(false);

        assertFalse(jsonLayout.format(loggingEvent()).endsWith("\n"));
    }

    @Test
    public void shouldSerializeWithTrainingNewline() {
        jsonLayout.setOneLogEventPerLine(true);

        assertTrue(jsonLayout.format(loggingEvent()).endsWith("\n"));
    }

    private LoggingEvent loggingEvent() {
        return new LoggingEvent("info", Logger.getLogger("name"), Priority.INFO, "message", null);
    }

}