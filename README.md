# Monitoring Azure Databricks in an Azure Log Analytics Workspace

This repository extends the core monitoring functionality of Azure Databricks to send streaming query event information to Azure Log Analytics. It has the following directory structure:

/src  
&nbsp;&nbsp;/spark-jobs  
&nbsp;&nbsp;/spark-listeners-loganalytics  
&nbsp;&nbsp;/spark-listeners  
&nbsp;&nbsp;/pom.xml

The **spark-jobs** directory is a sample Spark application with sample code demonstrating how to implement a Spark application metric counter.

The **spark-listeners-loganalytics** and **spark-listeners** directories contain the code for building the two JAR files that are deployed to the Databricks cluster. The **spark-listeners** directory includes a **scripts** directory that contains a cluster node initialization script to copy the JAR files from a staging directory in the Azure Databricks file system to execution nodes.

The **pom.xml** file is the main Maven project object model build file for the entire project.

## Build the Azure Databricks monitoring library and configure an Azure Databricks cluster

Before you begin, ensure you have the following prerequisites in place:

* Clone, fork, or download the [GitHub repository](https://github.com/mspnp/spark-monitoring).
* An active Azure Databricks workspace. For instructions on how to deploy an Azure Databricks workspace, see [get started with Azure Databricks.](https://docs.microsoft.com/azure/azure-databricks/quickstart-create-databricks-workspace-portal).
* Install the [Azure Databricks CLI](https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html#install-the-cli).
  * An Azure Databricks personal access token is required to use the CLI. For instructions, see [token management](https://docs.azuredatabricks.net/api/latest/authentication.html#token-management).
  * You can also use the Azure Databricks CLI from the Azure Cloud Shell.
* A Java IDE, with the following resources:
  * [Java Devlopment Kit (JDK) version 1.8](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
  * [Scala language SDK 2.11](https://www.scala-lang.org/download/)
  * [Apache Maven 3.5.4](http://maven.apache.org/download.cgi)

### Build the Azure Databricks monitoring library

To build the Azure Databricks monitoring library, follow these steps:

1. Import the Maven project project object model file, _pom.xml_, located in the **/src** folder into your project. This will import three projects:

* spark-jobs
* spark-listeners
* spark-listeners-loganalytics

2. Execute the Maven **package** build phase in your Java IDE to build the JAR files for each of the these three projects:

|Project| JAR file|
|-------|---------|
|spark-jobs|spark-jobs-1.0-SNAPSHOT.jar|
|spark-listeners|spark-listeners-1.0-SNAPSHOT.jar|
|spark-listeners-loganalytics|spark-listeners-loganalytics-1.0-SNAPSHOT.jar|

3. Use the Azure Databricks CLI to create a directory named **dbfs:/databricks/monitoring-staging**:  

  ```bash
  dbfs mkdirs dbfs:/databricks/monitoring-staging
  ```

4. Use the Azure Databricks CLI to copy **/src/spark-listeners/scripts/listeners.sh** to the directory created in step 3:

```bash
dbfs cp <local path to listeners.sh> dbfs:/databricks/monitoring-staging/listeners.sh
```

5. Use the Azure Databricks CLI to copy **/src/spark-listeners/scripts/metrics.properties** to the directory created in step 3:

```bash
dbfs cp <local path to metrics.properties> dbfs:/databricks/monitoring-staging/metrics.properties
```

6. Use the Azure Databricks CLI to copy **spark-listeners-1.0-SNAPSHOT.jar** and **spark-listeners-loganalytics-1.0-SNAPSHOT.jar** that were built in step 2 to the directory created in step 3:

```bash
dbfs cp <local path to spark-listeners-1.0-SNAPSNOT.jar> dbfs:/databricks/monitoring-staging/spark-listeners-1.0-SNAPSHOT.jar
dbfs cp <local path to spark-listeners-loganalytics-1.0-SNAPSHOT.jar> dbfs:/databricks/monitoring-staging/spark-listeners-loganalytics-1.0-SNAPSHOT.jar
```

### Create and configure the Azure Databricks cluster

To create and configure the Azure Databricks cluster, follow these steps:

1. Navigate to your Azure Databricks workspace in the Azure Portal.
2. On the home page, click "new cluster".
3. Choose a name for your cluster and enter it in "cluster name" text box. 
4. In the "Databricks Runtime Version" dropdown, select **4.3 (includes Apache Spark 2.3.1, Scala 2.11)**.
5. Under "Advanced Options", click on the "Spark" tab. Enter the following name-value pairs in the "Spark Config" text box:

  | Name | Value |
  |------|-------|
  |spark.extraListeners| com.databricks.backend.daemon.driver.DBCEventLoggingListener,org.apache.spark.listeners.UnifiedSparkListener|
  |spark.unifiedListener.sink |org.apache.spark.listeners.sink.loganalytics.LogAnalyticsListenerSink|
  |spark.unifiedListener.logBlockUpdates|false|
6. While still under the "Spark" tab, enter the following in the "Environment Variables" text box:
* LOG_ANALYTICS_WORKSPACE_ID=[your Azure Log Analytics workspace ID](/azure/azure-monitor/platform/agent-windows#obtain-workspace-id-and-key)
* LOG_ANALYTICS_WORKSPACE_KEY=[your Azure Log Analytics shared access signature](/azure/azure-monitor/platform/agent-windows#obtain-workspace-id-and-key)
7. While still under the "Advanced Options" section, click on the "Init Scripts" tab. Go to the last line under the "Init Scripts section" Under the "destination" dropdown, select "DBFS". Enter "dbfs:/databricks/monitoring-staging/listeners.sh" in the text box. Click the "add" button.
8. Click the "create cluster" button to create the cluster. Next, click on the "start" button to start the cluster.
