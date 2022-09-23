#!/bin/bash

set -e
set -o pipefail

# These environment variables would normally be set by Spark scripts
# However, for a Databricks init script, they have not been set yet.
# We will keep the names the same here, but not export them.
# These must be changed if the associated Spark environment variables
# are changed.
DB_HOME=/databricks
SPARK_HOME=$DB_HOME/spark
SPARK_CONF_DIR=$SPARK_HOME/conf

# Add your Log Analytics Workspace information below so all clusters use the same
# Log Analytics Workspace
# Also if it is available use AZ_* variables to include x-ms-AzureResourceId
# header as part of the request
tee -a "$SPARK_CONF_DIR/spark-env.sh" << EOF
export DB_CLUSTER_ID=$DB_CLUSTER_ID
export DB_CLUSTER_NAME=$DB_CLUSTER_NAME
export LOG_ANALYTICS_WORKSPACE_ID=
export LOG_ANALYTICS_WORKSPACE_KEY=
export AZ_SUBSCRIPTION_ID=
export AZ_RSRC_GRP_NAME=
export AZ_RSRC_PROV_NAMESPACE=
export AZ_RSRC_TYPE=
export AZ_RSRC_NAME=

# Note: All REGEX filters below are implemented with java.lang.String.matches(...).  This implementation essentially appends ^...$ around
# the regular expression, so the entire string must match the regex.  If you need to allow for other values you should include .* before and/or
# after your expression.

# Add a quoted regex value to filter the events for SparkListenerEvent_CL, the log will only include events where Event_s matches the regex.
# Commented example below will only log events for SparkListenerJobStart, SparkListenerJobEnd, or where "org.apache.spark.sql.execution.ui."
# is is the start of the event name.
# export LA_SPARKLISTENEREVENT_REGEX="SparkListenerJobStart|SparkListenerJobEnd|org\.apache\.spark\.sql\.execution\.ui\..*"
export LA_SPARKLISTENEREVENT_REGEX=${SPARK_LISTENER_EVENT_REGEX}

# Add a quoted regex value to filter the events for SparkMetric_CL, the log will only include events where name_s matches the regex.
# Commented example below will only log metrics where the name begins with app and ends in .jvmCpuTime or .heap.max.
# export LA_SPARKMETRIC_REGEX="app.*\.jvmCpuTime|app.*\.heap.max"
export LA_SPARKMETRIC_REGEX=${SPARK_METRIC_REGEX}

# Add a quoted regex value to filter the events for SparkLoggingEvent_CL, the log will only include events where logger_name_s matches the name regex
# or where the Message matches the message regex.  If both are specified, then both must be matched for the log to be sent.
# Commented examples below will only log messages where the logger name is com.microsoft.pnp.samplejob.StreamingQueryListenerSampleJob or begins with
# org.apache.spark.util.Utils, or where the Message ends with the string 'StreamingQueryListenerSampleJob' or begins with the string 'FS_CONF_COMPAT'.
# export LA_SPARKLOGGINGEVENT_NAME_REGEX="com\.microsoft\.pnp\.samplejob\.StreamingQueryListenerSampleJob|org\.apache\.spark\.util\.Utils.*"
# export LA_SPARKLOGGINGEVENT_MESSAGE_REGEX=".*StreamingQueryListenerSampleJob|FS_CONF_COMPAT.*"
export LA_SPARKLOGGINGEVENT_NAME_REGEX=${SPARK_LOGGING_EVENT_NAME_REGEX}
export LA_SPARKLOGGINGEVENT_MESSAGE_REGEX=${SPARK_LOGGING_EVENT_MESSAGE_REGEX}

# Uncomment the following line to enable local logging to stderr of buffer contents when an exception is hit when sending a buffer to Log Analytics
# export LA_LOGFAILEDBUFFERSEND=TRUE
EOF

STAGE_DIR=/environments/spark-monitoring
SPARK_LISTENERS_VERSION=${SPARK_LISTENERS_VERSION:-1.0.0}
SPARK_LISTENERS_LOG_ANALYTICS_VERSION=${SPARK_LISTENERS_LOG_ANALYTICS_VERSION:-1.0.0}
SPARK_VERSION=$(cat /databricks/spark/VERSION 2> /dev/null || echo "")
SPARK_VERSION=${SPARK_VERSION:-3.2.1}
SPARK_SCALA_VERSION=$(ls /databricks/spark/assembly/target | cut -d '-' -f2 2> /dev/null || echo "")
SPARK_SCALA_VERSION=${SPARK_SCALA_VERSION:-2.12}

# This variable configures the spark-monitoring library metrics sink.
# Any valid Spark metric.properties entry can be added here as well.
# It will get merged with the metrics.properties on the cluster.
METRICS_PROPERTIES=$(cat << EOF
# This will enable the sink for all of the instances.
*.sink.loganalytics.class=org.apache.spark.metrics.sink.loganalytics.LogAnalyticsMetricsSink
*.sink.loganalytics.period=5
*.sink.loganalytics.unit=seconds

# Enable JvmSource for instance master, worker, driver and executor
master.source.jvm.class=org.apache.spark.metrics.source.JvmSource

worker.source.jvm.class=org.apache.spark.metrics.source.JvmSource

driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource

executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource

EOF
)

echo "Copying Spark Monitoring jars"
JAR_FILENAME="spark-listeners_${SPARK_VERSION}_${SPARK_SCALA_VERSION}-${SPARK_LISTENERS_VERSION}.jar"
echo "Copying $JAR_FILENAME"
cp -f "$STAGE_DIR/$JAR_FILENAME" /mnt/driver-daemon/jars
JAR_FILENAME="spark-listeners-loganalytics_${SPARK_VERSION}_${SPARK_SCALA_VERSION}-${SPARK_LISTENERS_LOG_ANALYTICS_VERSION}.jar"
echo "Copying $JAR_FILENAME"
cp -f "$STAGE_DIR/$JAR_FILENAME" /mnt/driver-daemon/jars
echo "Copied Spark Monitoring jars successfully"

echo "Merging metrics.properties"
echo "$(echo "$METRICS_PROPERTIES"; cat "$SPARK_CONF_DIR/metrics.properties")" > "$SPARK_CONF_DIR/metrics.properties" || { echo "Error writing metrics.properties"; exit 1; }
echo "Merged metrics.properties successfully"

# This will enable master/worker metrics
cat << EOF >> "$SPARK_CONF_DIR/spark-defaults.conf"
spark.metrics.conf ${SPARK_CONF_DIR}/metrics.properties
EOF

log4jDirectories=( "executor" "driver" "master-worker" )
for log4jDirectory in "${log4jDirectories[@]}"
do

LOG4J_CONFIG_FILE="$SPARK_HOME/dbconf/log4j/$log4jDirectory/log4j.properties"
echo "BEGIN: Updating $LOG4J_CONFIG_FILE with Log Analytics appender"
sed -i 's/log4j.rootCategory=.*/&, logAnalyticsAppender/g' ${LOG4J_CONFIG_FILE}
tee -a ${LOG4J_CONFIG_FILE} << EOF
# logAnalytics
log4j.appender.logAnalyticsAppender=com.microsoft.pnp.logging.loganalytics.LogAnalyticsAppender
log4j.appender.logAnalyticsAppender.filter.spark=com.microsoft.pnp.logging.SparkPropertyEnricher
# Commented line below shows how to set the threshhold for logging to only capture events that are
# level ERROR or more severe.
# log4j.appender.logAnalyticsAppender.Threshold=ERROR
EOF

echo "END: Updating $LOG4J_CONFIG_FILE with Log Analytics appender"

done

# The spark.extraListeners property has an entry from Databricks by default.
# We have to readd it here because we did not find a way to get this setting when the init script is running.
# If Databricks changes the default value of this property, it needs to be changed here.
cat << EOF > "$DB_HOME/driver/conf/00-custom-spark-driver-defaults.conf"
[driver] {
    "spark.extraListeners" = "com.databricks.backend.daemon.driver.DBCEventLoggingListener,org.apache.spark.listeners.UnifiedSparkListener"
    "spark.unifiedListener.sink" = "org.apache.spark.listeners.sink.loganalytics.LogAnalyticsListenerSink"
}
EOF
