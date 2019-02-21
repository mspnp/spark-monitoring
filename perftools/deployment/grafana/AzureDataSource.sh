#!/usr/bin/env bash
sudo  /opt/bitnami/grafana/bin/grafana-cli --pluginsDir /opt/bitnami/grafana/data/plugins/ plugins install grafana-azure-monitor-datasource
sudo /opt/bitnami/ctlscript.sh restart grafana 