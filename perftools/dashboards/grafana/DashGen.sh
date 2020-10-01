#!/usr/bin/env bash

sed "s/YOUR_WORKSPACEID/${WORKSPACE}/g" "SparkMetricsDashboardTemplate.json"  | sed  "s/SparkListenerEvent_CL/${LOGTYPE}/g" > SparkMetricsDash.json
