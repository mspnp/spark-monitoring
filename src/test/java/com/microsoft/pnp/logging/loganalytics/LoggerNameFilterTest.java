package com.microsoft.pnp.logging.loganalytics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.test.appender.ListAppender;
import org.apache.logging.log4j.core.test.junit.LoggerContextSource;
import org.apache.logging.log4j.core.test.junit.Named;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;

import static org.junit.jupiter.api.Assertions.*;


@LoggerContextSource("log4j-loggernamefilter.xml")
public class LoggerNameFilterTest {

  private final ListAppender appender;
  private final LoggerNameFilter filter;
  private final Logger matchingLogger;
  private final Logger nonMatchingLogger;


  public LoggerNameFilterTest(final LoggerContext context, @Named("LIST") final ListAppender app) {
    this.appender = app;
    this.filter = (LoggerNameFilter)app.getFilter();
    this.matchingLogger = context.getLogger(getClass());
    this.nonMatchingLogger = context.getLogger("com.not.matching.logger");

  }

  /**
   * Test the case where the logger name matches the regex provided to the filter
   * and the one where it does not.
   */
  @Test
  public void testFilteringByNameIsOperational() {
    assertNotNull(filter);
    assertInstanceOf(LoggerNameFilter.class, filter);


    matchingLogger.info("This log message should be accepted by the filter.");
    // The appender should have only received the log message from the matching logger.
    assertEquals(1, appender.getMessages().size());
    appender.clear();
    nonMatchingLogger.info("This log message should not be accepted by the filter.");
    assertTrue(appender.getMessages().isEmpty());
  }


  @Test
  public void testInvalidRegexNoLog() {
    // We start with the correct configuration
    matchingLogger.info("This log message should be accepted by the filter.");
    assertEquals(1, appender.getMessages().size());
    appender.clear();
    // And switch to a failing configuration
    org.apache.logging.log4j.core.LoggerContext ctx =
            (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
    URL confURL = this.getClass().getClassLoader().getResource("log4j-loggernamefilter-invalidregexp.xml");
    try {
      ctx.setConfigLocation(confURL.toURI());
    } catch (Exception e) {
      Assertions.fail(e.getMessage());
    }
    ctx.updateLoggers();

    ctx.getLogger("foo").info("bar");
    // Note the  IllegalArgumentException raised due to invalid regex is catched by Log4j,  but hidden unless  log4j.debug property is set.
    matchingLogger.info("This log message should be accepted by the filter.");
    assertEquals(0, appender.getMessages().size());
  }

}