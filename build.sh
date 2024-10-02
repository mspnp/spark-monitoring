#! /bin/bash -eu

# Fix broken DNS bits
echo 'hosts: files dns' > /etc/nsswitch.conf
echo "127.0.0.1   $(hostname)" >> /etc/hosts

MAVEN_PROFILES=( "dbr-11.3-lts" "dbr-12.2-lts" "dbr-13.3-lts" "dbr-14.3-lts" "dbr-15.4-lts")
for MAVEN_PROFILE in "${MAVEN_PROFILES[@]}"
do
    mvn -f /spark-monitoring/pom.xml clean install -P ${MAVEN_PROFILE} "$@"
    cp /spark-monitoring/target/spark-monitoring_1.0.0.jar "/spark-monitoring/spark-monitoring_1.0.0.${MAVEN_PROFILE}.jar"
done
