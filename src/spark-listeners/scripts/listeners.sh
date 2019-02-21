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