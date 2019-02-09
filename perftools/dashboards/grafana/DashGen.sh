#!/usr/bin/env bash

sed  "/workspace/c\   \t\t\t\"workspace\" : \"${WORKSPACE}\"" "SparkMonitoringDashTemplate.json"  | sed  "s/SparkListenerEvent_CL/${LOGTYPE}/g"    > SparkMonitoringDash.json
