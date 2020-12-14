package com.microsoft.pnp.logging.loganalytics;

import com.microsoft.pnp.client.loganalytics.LogAnalyticsClient;
import junit.framework.TestCase;
import org.apache.http.client.HttpClient;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import static org.mockito.Mockito.*;

import static org.junit.Assert.*;

public class LogAnalyticsAppenderTest {

    Field nameregex;
    Field messageregex;
    LogAnalyticsAppender sut;
    LoggingEvent test = mock(LoggingEvent.class);

    @Before
    public void setUp() throws NoSuchFieldException, IllegalAccessException {
        sut = new LogAnalyticsAppender();

        // These fields are set private static from environment at startup, but we need different values for testing so
        // we are changing them to be accessible so that we can modify for testing.
        nameregex = LogAnalyticsAppender.class.getDeclaredField("LA_SPARKLOGGINGEVENT_NAME_REGEX");
        messageregex = LogAnalyticsAppender.class.getDeclaredField("LA_SPARKLOGGINGEVENT_MESSAGE_REGEX");
        nameregex.setAccessible(true);
        messageregex.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField( "modifiers" );
        modifiersField.setAccessible( true );
        modifiersField.setInt( nameregex, nameregex.getModifiers() & ~Modifier.FINAL );
        modifiersField.setInt( messageregex, messageregex.getModifiers() & ~Modifier.FINAL );
    }

    @Test
    public void FilterShouldWorkWithEmptyEnv() throws IllegalAccessException {
        nameregex.set(null,"");
        messageregex.set(null,"");
        when(test.getLoggerName()).thenReturn("12_This_is_a_test_34");
        when(test.getRenderedMessage()).thenReturn("This is a generic log message");
        assertEquals(sut.getFilter().decide(test),Filter.NEUTRAL);
    }
    @Test
    public void FilterShouldRejectOrgApacheHttp() throws IllegalAccessException {
        nameregex.set(null,"");
        messageregex.set(null,"");
        when(test.getLoggerName()).thenReturn("org.apache.http.this.is.a.test");
        when(test.getRenderedMessage()).thenReturn("This is a generic log message");
        assertEquals(sut.getFilter().decide(test),Filter.DENY);
    }
    @Test
    public void FilterShouldAllowNameRegex() throws IllegalAccessException {
        nameregex.set(null,"12.*34");
        messageregex.set(null,"");
        when(test.getLoggerName()).thenReturn("12_This_is_a_test_34");
        when(test.getRenderedMessage()).thenReturn("This is a generic log message");
        assertEquals(sut.getFilter().decide(test),Filter.NEUTRAL);
    }
    @Test
    public void FilterShouldDenyNameRegex() throws IllegalAccessException {
        nameregex.set(null,"12.*34");
        messageregex.set(null,"");
        when(test.getLoggerName()).thenReturn("x12_This_is_a_test_34x");
        when(test.getRenderedMessage()).thenReturn("This is a generic log message");
        assertEquals(sut.getFilter().decide(test),Filter.DENY);
        when(test.getLoggerName()).thenReturn("12_This_is_a_test_3456789");
        when(test.getRenderedMessage()).thenReturn("This is a generic log message");
        assertEquals(sut.getFilter().decide(test),Filter.DENY);
    }
    @Test
    public void FilterShouldDenyMessageRegex() throws IllegalAccessException {
        nameregex.set(null,"");
        messageregex.set(null,"(?i)((?!password).)*");// Only match if the message does not contain the word password
        when(test.getLoggerName()).thenReturn("12_This_is_a_test_34");
        when(test.getRenderedMessage()).thenReturn("This message is pretending to return a logged password: randomstring");
        assertEquals(sut.getFilter().decide(test),Filter.DENY);
        when(test.getRenderedMessage()).thenReturn("This message is pretending to return a logged Password: randomstring");
        assertEquals(sut.getFilter().decide(test),Filter.DENY);
    }
    @Test
    public void FilterShouldAllowMessageRegex() throws IllegalAccessException {
        nameregex.set(null,"");
        messageregex.set(null,"(?i)((?!password).)*");// Only match if the message does not contain the word password
        when(test.getLoggerName()).thenReturn("12_This_is_a_test_34");
        when(test.getRenderedMessage()).thenReturn("This message is pretending to return a logged Username: not_a_user");
        assertEquals(sut.getFilter().decide(test),Filter.NEUTRAL);
    }
     @Test
    public void FilterShouldDenyNameMessageRegex() throws IllegalAccessException {
        nameregex.set(null,"12.*34");
        messageregex.set(null,"(?i)((?!password).)*");// Only match if the message does not contain the word password
        // message does not match
        when(test.getLoggerName()).thenReturn("12_This_is_a_test_34");
        when(test.getRenderedMessage()).thenReturn("This message is pretending to return a logged password: randomstring");
        assertEquals(sut.getFilter().decide(test),Filter.DENY);
        // name doesn't match
        when(test.getLoggerName()).thenReturn("12_This_is_a_test_3456789");
        when(test.getRenderedMessage()).thenReturn("This is a generic log message");
        assertEquals(sut.getFilter().decide(test),Filter.DENY);
        // name and message do not match
        when(test.getLoggerName()).thenReturn("12_This_is_a_test_3456789");
        when(test.getRenderedMessage()).thenReturn("This message is pretending to return a logged password: randomstring");
        assertEquals(sut.getFilter().decide(test),Filter.DENY);
    }
}
