#! /bin/bash -eu

# Fix broken DNS bits
echo 'hosts: files dns' > /etc/nsswitch.conf
echo "127.0.0.1   $(hostname)" >> /etc/hosts

MAVEN_PROFILES=( "scala-2.11_spark-2.4.3" "scala-2.11_spark-2.4.5" "scala-2.12_spark-3.0.1" "scala-2.12_spark-3.1.1")
for MAVEN_PROFILE in "${MAVEN_PROFILES[@]}"
do
    mvn -f /spark-monitoring/src/pom.xml install -P ${MAVEN_PROFILE}
done
