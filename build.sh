#! /bin/bash -e

# Fix broken DNS bits
echo 'hosts: files dns' > /etc/nsswitch.conf
echo "127.0.0.1   $(hostname)" >> /etc/hosts

MAVEN_PROFILES=( "scala-2.11_spark-2.4.0" "scala-2.11_spark-2.4.1" "scala-2.11_spark-2.4.3" "scala-2.11_spark-2.4.4")
for MAVEN_PROFILE in "${MAVEN_PROFILES[@]}"
do
    if [[ -z $1 ]]
    then
        mvn -pl '!spark-sample-job' -f /spark-monitoring/src/pom.xml install -P ${MAVEN_PROFILE}
    else
        mvn -f /spark-monitoring/src/pom.xml install -P ${MAVEN_PROFILE}
    fi
done
