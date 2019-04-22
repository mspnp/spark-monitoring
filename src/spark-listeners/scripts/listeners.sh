#!/bin/bash

STAGE_DIR=/dbfs/databricks/monitoring-staging

# These environment variables would normally be set by Spark scripts
# However, for a Databricks init script, they have not been set yet.
# We will keep the names the same here, but not export them.
# These must be changed if the associated Spark environment variables
# are changed.
DB_HOME=/databricks
SPARK_HOME=$DB_HOME/spark
SPARK_CONF_DIR=$SPARK_HOME/conf

echo "Copying listener jar"
cp -f "$STAGE_DIR/spark-listeners-1.0-SNAPSHOT.jar" /mnt/driver-daemon/jars || { echo "Error copying file"; exit 1;}
cp -f "$STAGE_DIR/spark-listeners-loganalytics-1.0-SNAPSHOT.jar" /mnt/driver-daemon/jars || { echo "Error copying file"; exit 1;}
echo "Copied listener jar successfully"

echo "Merging metrics.properties"
cat "$STAGE_DIR/metrics.properties" <(echo) "$SPARK_CONF_DIR/metrics.properties" > "$SPARK_CONF_DIR/tmp.metrics.properties" || { echo "Error merging metrics.properties"; exit 1; }
mv "$SPARK_CONF_DIR/tmp.metrics.properties" "$SPARK_CONF_DIR/metrics.properties" || { echo "Error writing metrics.properties"; exit 1; }
echo "Merged metrics.properties successfully"

# This will enable master/worker metrics
cat << EOF >> "$SPARK_CONF_DIR/spark-defaults.conf"
spark.metrics.conf $SPARK_CONF_DIR/metrics.properties
EOF

log4jDirectories=( "executor" "driver" "master-worker" )
for log4jDirectory in "${log4jDirectories[@]}"
do

log4jConfigFile=$SPARK_HOME/dbconf/log4j/$log4jDirectory/log4j.properties
echo "BEGIN: Updating $log4jConfigFile with Log Analytics appender"
sed -i 's/log4j.rootCategory=.*/&, logAnalyticsAppender/g' $log4jConfigFile
tee -a $log4jConfigFile << EOF
# logAnalytics
log4j.appender.logAnalyticsAppender=com.microsoft.pnp.logging.loganalytics.LogAnalyticsAppender
log4j.appender.logAnalyticsAppender.filter.spark=com.microsoft.pnp.logging.SparkPropertyEnricher
EOF
echo "END: Updating $log4jConfigFile with Log Analytics appender"

done

cat << EOF > "$DB_HOME/driver/conf/00-custom-spark-driver-defaults.conf"
[driver] {
    "spark.extraListeners" = "com.databricks.backend.daemon.driver.DBCEventLoggingListener,org.apache.spark.listeners.UnifiedSparkListener"
    "spark.unifiedListener.sink" = "org.apache.spark.listeners.sink.loganalytics.LogAnalyticsListenerSink"
}
EOF

# Add your Log Analytics Workspace information below so all clusters use the same
# Log Analytics Workspace
tee -a "$SPARK_CONF_DIR/spark-env.sh" << EOF
export DB_CLUSTER_ID=$DB_CLUSTER_ID
export LOG_ANALYTICS_WORKSPACE_ID=
export LOG_ANALYTICS_WORKSPACE_KEY=
EOF
