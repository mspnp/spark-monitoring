# Filtering

## Introduction

The Spark Monitoring Library can generate large volumes of logging and metrics data.  This page describes the ways that you can limit the events that are forwarded to Azure Monitor.

> Note: All regex filters below are implemented with java.lang.String.matches(...). This implementation essentially appends ^...$ around the regular expression, so the entire string must match the regex.  If you need to allow for other values you should include .* before and/or after your expression.

> Note: The REGEX value(s) should be surrounded by double quotes as noted in the examples so that the characters in the regular expression(s) are not interpretted by the shell.

## Limiting events in SparkListenerEvent_CL

You can uncomment and edit the `LA_SPARKLISTENEREVENT_REGEX` environment variable that is included in [spark-monitoring.sh](../src/spark-listeners/scripts/spark-monitoring.sh) to limit the logging to only include events where Event_s matches the regex.

The example below will only log events for `SparkListenerJobStart`, `SparkListenerJobEnd`, or where `org.apache.spark.sql.execution.ui.` is in the event name.

`export LA_SPARKLISTENEREVENT_REGEX="SparkListenerJobEnd|SparkListenerTaskEnd|org\.apache\.spark\.sql\.execution\.ui\..*"`

### Finding Event Names in Azure Monitor

The following query will show counts by day for all events that have been logged to Azure Monitor:
```kusto
SparkListenerEvent_CL
| project TimeGenerated, Event_s
| summarize Count=count() by tostring(Event_s), bin(TimeGenerated, 1d)
```

### Events Noted in SparkListenerEvent_CL

* SparkListenerExecutorAdded
* SparkListenerBlockManagerAdded
* org.apache.spark.sql.streaming.StreamingQueryListener$QueryStartedEvent
* org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart
* org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
* org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates
* org.apache.spark.sql.streaming.StreamingQueryListener$QueryTerminatedEvent
* SparkListenerJobStart
* SparkListenerStageSubmitted
* SparkListenerTaskStart
* SparkListenerTaskEnd
* org.apache.spark.sql.streaming.StreamingQueryListener$QueryProgressEvent
* SparkListenerStageCompleted
* SparkListenerJobEnd

## Limiting Metrics in SparkMetric_CL

You can uncomment and edit the `LA_SPARKMETRIC_REGEX` environment variable that is included in [spark-monitoring.sh](../src/spark-listeners/scripts/spark-monitoring.sh) to limit the logging to only include events where name_s matches the regex.

The example below will only log metrics where the name begins with `app` and ends in `.jvmCpuTime` or `.heap.max`.

`export LA_SPARKMETRIC_REGEX="app.*\.jvmCpuTime|app.*\.heap.max`

### Finding Metric Names in Azure Monitor

Query to find all metric prefixes and counts by day:

```kusto
SparkMetric_CL
| project nameprefix=split(tostring(name_s),".")[0], TimeGenerated
| summarize Count=count() by tostring(nameprefix), bin(TimeGenerated, 1d)
```
If you want to get more granular, the full names can be seen with the following query. Note: This will include a large number of metrics including for specific Spark applications.

```kusto
SparkMetric_CL
| project name_s, TimeGenerated
| summarize Count=count() by tostring(name_s), bin(TimeGenerated, 1d)
```

### Metric Name Prefixes Noted in SparkMetric_CL

* jvm
* worker
* Databricks
* HiveExternalCatalog
* CodeGenerator
* application
* master
* app-20201014133042-0000 - Note: This prefix includes all metrics for a specific Spark application run.
* shuffleService
* SparkStatusTracker

## Limiting Logs in SparkLoggingEvent_CL (Basic)

The logs that propagate to SparkLoggingEvent_CL do so through a log4j appender.  This can be configured by altering the spark-monitoring.sh script that is responsible for writing the log4j.properties file. The script at [spark-monitoring.sh](../src/spark-listeners/scripts/spark-monitoring.sh) can be modified to set the threshold for events to be forwarded.  A commented example is included in the script.

```bash
# Commented line below shows how to set the threshhold for logging to only capture events that are
# level ERROR or more severe.
# log4j.appender.logAnalyticsAppender.Threshold=ERROR
```

## Limiting Logs in SparkLoggingEvent_CL (Advanced)

You can uncomment and edit the `LA_SPARKLOGGINGEVENT_NAME_REGEX` environment variable that is included in [spark-monitoring.sh](../src/spark-listeners/scripts/spark-monitoring.sh) to limit the logging to only include events where logger_name_s matches the regex.

The example below will only log events from logger `com.microsoft.pnp.samplejob.StreamingQueryListenerSampleJob` or where the logger name starts with `org.apache.spark.util.Utils`.

`export LA_SPARKLOGGINGEVENT_NAME_REGEX="com\.microsoft\.pnp\.samplejob\.StreamingQueryListenerSampleJob|org\.apache\.spark\.util\.Utils.*"`

You can uncomment and edit the `LA_SPARKLOGGINGEVENT_MESSAGE_REGEX` environment variable that is included in [spark-monitoring.sh](../src/spark-listeners/scripts/spark-monitoring.sh) to limit the logging to only include events where the message matches the regex.

The example below will only log events where the message ends with the string `StreamingQueryListenerSampleJob` or begins with the string `FS_CONF_COMPAT`.

`export LA_SPARKLOGGINGEVENT_MESSAGE_REGEX=".*StreamingQueryListenerSampleJob|FS_CONF_COMPAT.*"`

## Performance Considerations

You should be mindful of using complicated Regular Expressions as they have to be evaluated for every logged event or metric.  Simple whole-string matches should be relatively performent, or `pattern.*` expressions that will match the beginning of strings.

> Warning: Filtering on the logging event message with `LA_SPARKLOGGINGEVENT_MESSAGE_REGEX` should be considered experimental. Some components generate very large message strings and processing the `.matches` operation on these strings could cause a significant burden on the cluster nodes.
