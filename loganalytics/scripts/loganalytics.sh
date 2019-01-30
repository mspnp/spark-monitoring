#!/bin/bash

STAGE_DIR=/dbfs/databricks/init/jar
echo "Copying loganalytics jar"
cp -f "$STAGE_DIR/loganalytics-1.0-SNAPSHOT.jar" /mnt/driver-daemon/jars || { echo "Error copying file"; exit 1;}
echo "Copied loganalytics jar successfully"