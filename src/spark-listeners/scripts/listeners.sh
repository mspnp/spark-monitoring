#!/bin/bash

STAGE_DIR=/dbfs/databricks/monitoring-staging
echo "Copying listener jar"
cp -f "$STAGE_DIR/spark-listeners-1.0-SNAPSHOT.jar" /mnt/driver-daemon/jars || { echo "Error copying file"; exit 1;}
cp -f "$STAGE_DIR/spark-listeners-loganalytics-1.0-SNAPSHOT.jar" /mnt/driver-daemon/jars || { echo "Error copying file"; exit 1;}
echo "Copied listener jar successfully"

echo "Merging metrics.properties"
cat "$STAGE_DIR/metrics.properties" <(echo) /databricks/spark/conf/metrics.properties > /databricks/spark/conf/tmp.metrics.properties || { echo "Error merging metrics.properties"; exit 1; }
mv /databricks/spark/conf/tmp.metrics.properties /databricks/spark/conf/metrics.properties || { echo "Error writing metrics.properties"; exit 1; }
echo "Merged metrics.properties successfully"

echo "BEGIN: Updating Executor log4j properties file with Log analytics appender"
sed -i 's/log4j.rootCategory=.*/&, logAnalyticsAppender/g' /databricks/spark/dbconf/log4j/executor/log4j.properties
tee -a /databricks/spark/dbconf/log4j/executor/log4j.properties << EOF
# logAnalytics
log4j.appender.logAnalyticsAppender=com.microsoft.pnp.logging.loganalytics.LogAnalyticsAppender
log4j.appender.logAnalyticsAppender.layout=com.microsoft.pnp.logging.JSONLayout
log4j.appender.logAnalyticsAppender.layout.LocationInfo=false
EOF
echo "END: Updating Executor log4j properties file with Log analytics appender"

echo "BEGIN: Updating Driver log4j properties file with Log analytics appender"
sed -i 's/log4j.rootCategory=.*/&, logAnalyticsAppender/g' /databricks/spark/dbconf/log4j/driver/log4j.properties
tee -a /databricks/spark/dbconf/log4j/driver/log4j.properties << EOF
# logAnalytics
log4j.appender.logAnalyticsAppender=com.microsoft.pnp.logging.loganalytics.LogAnalyticsAppender
log4j.appender.logAnalyticsAppender.layout=com.microsoft.pnp.logging.JSONLayout
log4j.appender.logAnalyticsAppender.layout.LocationInfo=false
EOF
echo "END: Updating Driver log4j properties file with Log analytics appender"

# Promote the DB_CLUSTER_ID into the environment so we can access it later.
echo CLUSTER_ID=$DB_CLUSTER_ID >> /etc/environment
