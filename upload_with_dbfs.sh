if [[ $# -eq 0 ]] ; then
    echo 'Parameter0 not supplied. Please include the version of the library. For example: ./upload_with_dbfs.sh 3.1.2_2.12-1.0.0. '
    exit 1
fi
dbfs mkdirs dbfs:/databricks/spark-monitoring
dbfs cp --overwrite src/target/spark-listeners_$1.jar dbfs:/databricks/spark-monitoring/
dbfs cp --overwrite src/target/spark-listeners-loganalytics_$1.jar dbfs:/databricks/spark-monitoring/
dbfs cp --overwrite src/spark-listeners/scripts/spark-monitoring.sh dbfs:/databricks/spark-monitoring/