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
echo '************************************************************************************'
echo 'To install the library, add the following init script:'
echo '    dbfs:/databricks/spark-monitoring/spark-monitoring.sh'
echo ' And include the following Environment Variables:'
echo '    LOG_ANALYTICS_WORKSPACE_KEY={{secrets/your-scope-linked-to-keyvault/databricksloganalytics-key}}'
echo '    LOG_ANALYTICS_WORKSPACE_ID={{secrets/your-scope-linked-to-keyvault/databricksloganalytics-workspace-id}}'
echo '    LA_SPARKMETRIC_REGEX=app.*\.ExternalShuffle\.shuffle-client\.usedDirectMemory|app.*\.jvm\.pools\.PS-Eden-Space\.used|app.*\.jvm\.pools\.PS-Old-Gen\.used|app.*\.jvm\.total\.used|app.*\.jvm\.pools\.PS-Survivor-Space\.used|app.*\.jvm\.pools\.PS-Old-Gen\.used-after-gc|app.*\.jvm\.pools\.Code-Cache\.used|app.*\.jvm\.pools\.Metaspace\.used    '
echo '    LA_SPARKLISTENEREVENT_REGEX=SparkListenerTaskEnd|SparkListenerExecutorAdded|SparkListenerBlockManagerAdded|SparkListenerJobStart|SparkListenerStageSubmitted|SparkListenerTaskGettingResult|SparkListenerTaskStart   '
echo '************************************************************************************'
