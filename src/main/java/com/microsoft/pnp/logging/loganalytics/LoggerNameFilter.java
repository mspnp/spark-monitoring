package com.microsoft.pnp.logging.loganalytics;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.message.Message;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * This custom filter is used to filter log events based on the logger name.
 * It is designed to be used only at appender level.
 * The "regex" attribute is used to specify the regular expression to match the entire logger name.
 *
 */
@Plugin(name = "LoggerNameFilter",
    category = Node.CATEGORY,
    elementType = Filter.ELEMENT_TYPE,
    printObject = true)
public class LoggerNameFilter extends AbstractFilter {

  private final Pattern pattern;

  private LoggerNameFilter(final Pattern pattern, final Result onMatch, final Result onMismatch) {
    super(onMatch, onMismatch);
    this.pattern = pattern;
  }

  @Override
  public Result filter(LogEvent event) {
    return filter(event.getLoggerName());
  }

  @Override
  public Result filter(Logger logger, Level level, Marker marker, Message msg, Throwable t) {
    return filter(logger.getName());
  }

  @Override
  public Result filter(Logger logger, Level level, Marker marker, Object msg, Throwable t) {
    return filter(logger.getName());
  }

  @Override
  public Result filter(Logger logger, Level level, Marker marker, String msg, Object... params) {
    return filter(logger.getName());
  }

  private Result filter(final String loggerName) {
    if (loggerName != null && pattern.matcher(loggerName).matches()) {
      return onMatch;
    }
    return onMismatch;
  }

  @PluginBuilderFactory
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder extends AbstractFilterBuilder<Builder>
      implements org.apache.logging.log4j.core.util.Builder<LoggerNameFilter> {

      @PluginBuilderAttribute
      private String regex;

      public Builder setRegex(String regex) {
        this.regex = regex;
        return this;
      }

      @Override
      public LoggerNameFilter build() throws IllegalArgumentException{
          if (regex == null) {
              LOGGER.warn("LoggerNameFilter regex has been defaulted to .*");
              regex = ".*";
          }
          try {
              return new LoggerNameFilter(Pattern.compile(regex), getOnMatch(), getOnMismatch());
          } catch (PatternSyntaxException pse) {
              String message = "Could not compile the LoggerNameFilter regex : " + pse.getMessage();
              LOGGER.error(message);
              throw new IllegalArgumentException(message);
          }
      }
  }
}
