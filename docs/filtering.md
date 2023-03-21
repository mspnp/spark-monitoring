# Filtering

## Introduction

The Spark Monitoring Library can generate large volumes of logging and metrics data.  This page describes the ways that you can limit the events that are forwarded to Azure Monitor.

> Note: All regex filters below are implemented with java.lang.String.matches(...). This implementation essentially appends ^...$ around the regular expression, so the entire string must match the regex.  If you need to allow for other values you should include .* before and/or after your expression.

> Note: The REGEX value(s) should be surrounded by double quotes as noted in the examples so that the characters in the regular expression(s) are not interpretted by the shell.

## Adding or removing fiels in SparkEvents_CL

In SparkLayout.json, you can specify your own template that would be use for logging.  
The syntax used is the same as in log4j2 JSON Template: https://logging.apache.org/log4j/2.x/manual/json-template-layout.html.  
As provided in SparkLayout.json, you can also add your own Spark variables, the full list can be found in the Environment tab on the SparkUi

## Adding or limiting events in SparkListenerEvent_CL

You can add/remove listener events that are sent to Azure LA by adding / removing methods in either:

- DatabricksListener.scala
- DatabricksQueryExecutionListener.scala
- DatabricksStreamingListener.scala
- DatabricksStreamingQueryListener.scala

### Events provided in SparkListenerEvent_CL

* org.apache.spark.scheduler.SparkListenerJobStart
* org.apache.spark.scheduler.SparkListenerJobEnd
* org.apache.spark.scheduler.SparkListenerApplicationStart
* org.apache.spark.scheduler.SparkListenerApplicationEnd

### Finding Event Names in Azure Monitor

The following query will show counts by day for all events that have been logged to Azure Monitor:
```kusto
SparkListenerEvent_CL
| project TimeGenerated, Event_s
| summarize Count=count() by tostring(Event_s), bin(TimeGenerated, 1d)
```

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


## Performance Considerations

You should be mindful of using complicated Regular Expressions as they have to be evaluated for every logged event or metric.  Simple whole-string matches should be relatively performent, or `pattern.*` expressions that will match the beginning of strings.

> Warning: Filtering on the logging event message with `LA_SPARKLOGGINGEVENT_MESSAGE_REGEX` should be considered experimental. Some components generate very large message strings and processing the `.matches` operation on these strings could cause a significant burden on the cluster nodes.
