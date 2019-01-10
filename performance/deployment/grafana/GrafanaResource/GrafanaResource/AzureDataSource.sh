#!/usr/bin/env bash


 sudo /opt/bitnami/ctlscript.sh restart grafana 
 sudo su grafana -c '/opt/bitnami/grafana/bin/grafana-cli --pluginsDir /opt/bitnami/grafana/data/plugins/ plugins install grafana-azure-monitor-datasource'