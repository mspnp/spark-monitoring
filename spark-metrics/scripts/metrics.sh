#!/bin/bash

STAGE_DIR=/dbfs/databricks/init/spark-listeners-test
#echo "Copying metrics jar"
#cp -f "$STAGE_DIR/spark-metrics-1.0-SNAPSHOT.jar" /mnt/driver-daemon/jars || { echo "Error copying file"; exit 1;}
#echo "Copied metrics jar successfully"

echo "Merging metrics.properties"
cat "$STAGE_DIR/metrics.properties" <(echo) /databricks/spark/conf/metrics.properties > /databricks/spark/conf/tmp.metrics.properties || { echo "Error merging metrics.properties"; exit 1; }
mv /databricks/spark/conf/tmp.metrics.properties /databricks/spark/conf/metrics.properties || { echo "Error writing metrics.properties"; exit 1; }
echo "Merged metrics.properties successfully"
