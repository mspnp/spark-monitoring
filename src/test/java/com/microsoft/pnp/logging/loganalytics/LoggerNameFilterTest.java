package com.microsoft.pnp.logging.loganalytics;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.test.appender.ListAppender;
import org.apache.logging.log4j.core.test.junit.LoggerContextSource;
import org.apache.logging.log4j.core.test.junit.Named;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;



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
  public void test() {
    assertNotNull(filter);
    assertInstanceOf(LoggerNameFilter.class, filter);


    matchingLogger.info("This log message should be accepted by the filter.");
    // The appender should have only received the log message from the matching logger.
    assertEquals(1, appender.getMessages().size());
    appender.clear();
    nonMatchingLogger.info("This log message should not be accepted by the filter.");
    assertTrue(appender.getMessages().isEmpty());
  }

}