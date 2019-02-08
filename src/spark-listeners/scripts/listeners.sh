#!/bin/bash

STAGE_DIR=/dbfs/databricks/init/spark-listeners-test
echo "Copying listener jar"
cp -f "$STAGE_DIR/spark-listeners-1.0-SNAPSHOT.jar" /mnt/driver-daemon/jars || { echo "Error copying file"; exit 1;}
cp -f "$STAGE_DIR/spark-listeners-loganalytics-1.0-SNAPSHOT.jar" /mnt/driver-daemon/jars || { echo "Error copying file"; exit 1;}
echo "Copied listener jar successfully"
