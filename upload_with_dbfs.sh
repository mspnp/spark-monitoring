if [[ $# -ne 2 ]] ; then
    echo 'Required 2 arguments: databricks_profile_name and version_of_library.'
    echo ' For example: ./upload_with_dbfs.sh ecbpremium 3.1.2_2.12-1.0.0 '
    exit 1
fi
echo 'Uploading to dbfs:/databricks/spark-monitoring/'
dbfs --profile $1 mkdirs dbfs:/databricks/spark-monitoring
dbfs --profile $1 cp --overwrite src/target/spark-listeners_$2.jar dbfs:/databricks/spark-monitoring/
dbfs --profile $1 cp --overwrite src/target/spark-listeners-loganalytics_$2.jar dbfs:/databricks/spark-monitoring/
dbfs --profile $1 cp --overwrite src/spark-listeners/scripts/spark-monitoring.sh dbfs:/databricks/spark-monitoring/

echo 'Uploaded to dbfs:/databricks/spark-monitoring/'
echo 'Showing the content of the folder dbfs:/databricks/spark-monitoring/'
dbfs --profile $1 ls dbfs:/databricks/spark-monitoring/