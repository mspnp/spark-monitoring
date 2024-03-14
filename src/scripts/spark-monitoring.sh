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

mkdir -p $SPARK_CONF_DIR
cat << 'EOF' >> "$SPARK_CONF_DIR/spark-env.sh"

export DB_CLUSTER_ID=$DB_CLUSTER_ID
export DB_CLUSTER_NAME=$DB_CLUSTER_NAME
export LOG_ANALYTICS_WORKSPACE_ID=
export LOG_ANALYTICS_WORKSPACE_KEY=
export AZ_SUBSCRIPTION_ID=
export AZ_RSRC_GRP_NAME=
export AZ_RSRC_PROV_NAMESPACE=
export AZ_RSRC_TYPE=
export AZ_RSRC_NAME=
EOF

STAGE_DIR=/dbfs/databricks/spark-monitoring
SPARK_MONITORING_VERSION=${SPARK_MONITORING_VERSION:-1.0.0}
SPARK_VERSION=$(cat /databricks/spark/VERSION 2> /dev/null || echo "")
SPARK_VERSION=${SPARK_VERSION:-3.4.1}
SPARK_SCALA_VERSION=$(ls /databricks/spark/assembly/target | cut -d '-' -f2 2> /dev/null || echo "")
SPARK_SCALA_VERSION=${SPARK_SCALA_VERSION:-2.12}

# This variable configures the spark-monitoring library metrics sink.
# Any valid Spark metric.properties entry can be added here as well.
# It will get merged with the metrics.properties on the cluster.
METRICS_PROPERTIES=$(cat << EOF
# This will enable the sink for all of the instances.
*.sink.loganalytics.class=org.apache.spark.metrics.pnp.LogAnalyticsMetricsSink
*.sink.loganalytics.period=5
*.sink.loganalytics.unit=seconds

# Enable JvmSource for instance master, worker, driver and executor
master.source.jvm.class=org.apache.spark.metrics.source.JvmSource

worker.source.jvm.class=org.apache.spark.metrics.source.JvmSource

driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource

executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource

EOF
)

JAR_FILENAME="spark-monitoring_${SPARK_MONITORING_VERSION}.jar"
echo "Copying $JAR_FILENAME"
cp -f "$STAGE_DIR/$JAR_FILENAME" /mnt/driver-daemon/jars
echo "Copied Spark Monitoring jars successfully"

echo "Copying log4j-layout-template-json-2.17.2.jar"
wget --quiet -O /mnt/driver-daemon/jars/log4j-layout-template-json-2.17.2.jar https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-layout-template-json/2.17.2/log4j-layout-template-json-2.17.2.jar
echo "Copied log4j-layout-template-json-2.17.2.jar"

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

LOG4J_CONFIG_FILE="$SPARK_HOME/dbconf/log4j/$log4jDirectory/log4j2.xml"
echo "BEGIN: Updating $LOG4J_CONFIG_FILE with Log Analytics appender"
CONTENT="\ \ <LogAnalyticsAppender name=\"logAnalyticsAppender\">\n\ \ \ \ <JsonTemplateLayout eventTemplateUri=\"file:${STAGE_DIR}/sparkLayout.json\"/>\n\ \ </LogAnalyticsAppender>"
C=$(echo $CONTENT | sed 's/\//\\\//g')
sed -i "/<\/Appenders>/ s/.*/${C}\n&/" $LOG4J_CONFIG_FILE

CONTENT="\ \ \ \ \ \ <AppenderRef ref=\"logAnalyticsAppender\"/>"
C=$(echo $CONTENT | sed 's/\//\\\//g')
sed -i "/<\/Root>/ s/.*/${C}\n&/" $LOG4J_CONFIG_FILE

sed -i 's/packages="\([^"]*\)"/packages="\1,com.microsoft.pnp.logging.loganalytics"/' $LOG4J_CONFIG_FILE

echo "END: Updating $LOG4J_CONFIG_FILE with Log Analytics appender"

done

# The spark.extraListeners property has an entry from Databricks by default.
# We have to read it here because we did not find a way to get this setting when the init script is running.
# If Databricks changes the default value of this property, it needs to be changed here.
mkdir -p $DB_HOME/driver/conf
cat << EOF > "$DB_HOME/driver/conf/00-custom-spark-driver-defaults.conf"
[driver] {
    "spark.extraListeners" = "com.databricks.backend.daemon.driver.DBCEventLoggingListener,com.microsoft.pnp.listeners.DatabricksListener"
}
EOF

mkdir -p $STAGE_DIR
cat << 'EOF' > "$STAGE_DIR/sparkLayout.json"
{
  "@timestamp": {
    "$resolver": "timestamp",
    "pattern": {
      "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
      "timeZone": "UTC"
    }
  },
  "ecs.version": "1.2.0",
  "log.level": {
    "$resolver": "level",
    "field": "name"
  },
  "message": {
    "$resolver": "message",
    "stringified": true
  },
  "process.thread.name": {
    "$resolver": "thread",
    "field": "name"
  },
  "log.logger": {
    "$resolver": "logger",
    "field": "name"
  },
  "labels": {
    "$resolver": "mdc",
    "flatten": true,
    "stringified": true
  },
  "tags": {
    "$resolver": "ndc"
  },
  "error.type": {
    "$resolver": "exception",
    "field": "className"
  },
  "error.message": {
    "$resolver": "exception",
    "field": "message"
  },
  "error.stack_trace": {
    "$resolver": "exception",
    "field": "stackTrace",
    "stackTrace": {
      "stringified": true
    }
  },
  "sparkAppName": "${spark:spark.app.name:-}",
  "sparkNode": "${spark:nodeType}"
}
EOF

