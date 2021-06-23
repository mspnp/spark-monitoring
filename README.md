# Monitoring Azure Databricks in an Azure Log Analytics Workspace

This repository extends the core monitoring functionality of Azure Databricks to send streaming query event information to Azure Monitor. For more information about using this library to monitor Azure Databricks, see [Monitoring Azure Databricks](https://docs.microsoft.com/azure/architecture/databricks-monitoring)

The project has the following directory structure:

```shell
/src
    /spark-listeners-loganalytics
    /spark-listeners
    /pom.xml
/sample
    /spark-sample-job
/perftools
     /spark-sample-job
```

The **spark-listeners-loganalytics** and **spark-listeners** directories contain the code for building the two JAR files that are deployed to the Databricks cluster. The **spark-listeners** directory includes a **scripts** directory that contains a cluster node initialization script to copy the JAR files from a staging directory in the Azure Databricks file system to execution nodes. The **pom.xml** file is the main Maven project object model build file for the entire project.

The **spark-sample-job** directory is a sample Spark application demonstrating how to implement a Spark application metric counter.

The **perftools** directory contains details on how to use Azure Monitor with Grafana to monitor Spark performance.

## Prerequisites

Before you begin, ensure you have the following prerequisites in place:

* Clone or download this GitHub repository.
* An active Azure Databricks workspace. For instructions on how to deploy an Azure Databricks workspace, see [get started with Azure Databricks.](https://docs.microsoft.com/azure/azure-databricks/quickstart-create-databricks-workspace-portal).
* Install the [Azure Databricks CLI](https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html#install-the-cli).
  * An Azure Databricks personal access token is required to use the CLI. For instructions, see [token management](https://docs.azuredatabricks.net/api/latest/authentication.html#token-management).
  * You can also use the Azure Databricks CLI from the Azure Cloud Shell.
* A Java IDE, with the following resources:
  * [Java Devlopment Kit (JDK) version 1.8](https://www.oracle.com/technetwork/java/javase/downloads/index.html)
  * [Scala language SDK 2.11 and/or 2.12](https://www.scala-lang.org/download/)
  * [Apache Maven 3.6.3](https://maven.apache.org/download.html)

## Logging Event Size Limit

This library currently has a size limit per event of 25MB, based on the [Log Analytics limit of 30MB per API Call](https://docs.microsoft.com/rest/api/loganalytics/create-request#data-limits) with additional overhead for formatting. The default behavior when hitting this limit is to throw an exception. This can be changed by modifying the value of `EXCEPTION_ON_FAILED_SEND` in [GenericSendBuffer.java](src/spark-listeners/src/main/java/com/microsoft/pnp/client/GenericSendBuffer.java) to `false`.

> Note: You will see an error like: `java.lang.RuntimeException: Failed to schedule batch because first message size nnn exceeds batch size limit 26214400 (bytes).` in the Spark logs if your workload is generating logging messages of greater than 25MB, and your workload may not proceed. You can query Log Analytics for this error condition with:

> ```kusto
> SparkLoggingEvent_CL
> | where TimeGenerated > ago(24h)
> | where Message contains "java.lang.RuntimeException: Failed to schedule batch because first message size"
> ```

## Build the Azure Databricks monitoring library

You can build the library using either Docker or Maven.  All commands are intended to be run from the base directory of the repository.

### Option 1: Docker

Linux:

```bash
# To build all profiles:
chmod +x ./build.sh
docker run -it --rm -v `pwd`:/spark-monitoring -v "$HOME/.m2":/root/.m2 maven:3.6.3-jdk-8 /spark-monitoring/build.sh

# To build a single profile (latest long term support version):
docker run -it --rm -v `pwd`:/spark-monitoring -v "$HOME/.m2":/root/.m2 -w /spark-monitoring/src maven:3.6.3-jdk-8 mvn install -P "scala-2.12_spark-3.0.1"
```

Windows:

```bash
# To build all profiles:
docker run -it --rm -v %cd%:/spark-monitoring -v "%USERPROFILE%/.m2":/root/.m2 maven:3.6.3-jdk-8 /spark-monitoring/build.sh

# To build a single profile (latest long term support version):
docker run -it --rm -v %cd%:/spark-monitoring -v "%USERPROFILE%/.m2":/root/.m2 -w /spark-monitoring/src maven:3.6.3-jdk-8 mvn install -P "scala-2.12_spark-3.0.1"
```

### Option 2: Maven

1. Import the Maven project project object model file, _pom.xml_, located in the **/src** folder into your project. This will import two projects:

    * spark-listeners
    * spark-listeners-loganalytics

1. Activate a **single** Maven profile that corresponds to the versions of the Scala/Spark combination that is being used. By default, the Scala 2.12 and Spark 3.0.1 profile is active.

1. Execute the Maven **package** phase in your Java IDE to build the JAR files for each of the these projects:

    |Project| JAR file|
    |-------|---------|
    |spark-listeners|`spark-listeners_<Spark Version>_<Scala Version>-<Version>.jar`|
    |spark-listeners-loganalytics|`spark-listeners-loganalytics_<Spark Version>_<Scala Version>-<Version>.jar`|

## Configure the Databricks workspace

Copy the JAR files and init scripts to Databricks.

1. Use the Azure Databricks CLI to create a directory named **dbfs:/databricks/spark-monitoring**:

    ```bash
    dbfs mkdirs dbfs:/databricks/spark-monitoring
    ```

1. Open the **/src/spark-listeners/scripts/spark-monitoring.sh** script file and add your [Log Analytics Workspace ID and Key](http://docs.microsoft.com/azure/azure-monitor/platform/agent-windows#obtain-workspace-id-and-key) to the lines below:

    ```bash
    export LOG_ANALYTICS_WORKSPACE_ID=
    export LOG_ANALYTICS_WORKSPACE_KEY=
    ```

If you do not want to add your Log Analytics workspace id and key into the init script in plaintext, you can also [create an Azure Key Vault backed secret scope](./docs/keyvault-backed-secrets.md) and reference those secrets through your cluster's environment variables.

1. In order to add `x-ms-AzureResourceId` [header](https://docs.microsoft.com/azure/azure-monitor/platform/data-collector-api#request-headers) as part of the http request, modify the following environment
variables on **/src/spark-listeners/scripts/spark-monitoring.sh**.
For instance:

```bash
export AZ_SUBSCRIPTION_ID=11111111-5c17-4032-ae54-fc33d56047c2
export AZ_RSRC_GRP_NAME=myAzResourceGroup
export AZ_RSRC_PROV_NAMESPACE=Microsoft.Databricks
export AZ_RSRC_TYPE=workspaces
export AZ_RSRC_NAME=myDatabricks
```

Now the _ResourceId **/subscriptions/11111111-5c17-4032-ae54-fc33d56047c2/resourceGroups/myAzResourceGroup/providers/Microsoft.Databricks/workspaces/myDatabricks** will be part of the header.
(*Note: If at least one of them is not set the header won't be included.*)

1. Use the Azure Databricks CLI to copy **src/spark-listeners/scripts/spark-monitoring.sh** to the directory created in step 3:

    ```bash
    dbfs cp src/spark-listeners/scripts/spark-monitoring.sh dbfs:/databricks/spark-monitoring/spark-monitoring.sh
    ```

1. Use the Azure Databricks CLI to copy all of the jar files from the **src/target** folder to the directory created in step 3:

    ```bash
    dbfs cp --overwrite --recursive src/target/*.jar dbfs:/databricks/spark-monitoring/
    ```

### Create and configure the Azure Databricks cluster

1. Navigate to your Azure Databricks workspace in the Azure Portal.
1. On the home page, click "new cluster".
1. Choose a name for your cluster and enter it in "cluster name" text box.
1. In the "Databricks Runtime Version" dropdown, select **7.3 LTS (includes Apache Spark 3.0.1, Scala 2.12)**.
1. Under "Advanced Options", click on the "Init Scripts" tab. Go to the last line under the "Init Scripts section" Under the "destination" dropdown, select "DBFS". Enter "dbfs:/databricks/spark-monitoring/spark-monitoring.sh" in the text box. Click the "add" button.
1. Click the "create cluster" button to create the cluster. Next, click on the "start" button to start the cluster.

## Run the sample job (optional)

The repository includes a sample application that shows how to send application metrics and application logs to Azure Monitor.

When building the sample job, specify a maven profile compatible with your
databricks runtime.

| Databricks Runtime(s) | Maven Profile |
| -- | -- |
| `5.5` | `scala-2.11_spark-2.4.3` |
| `6.4` | `scala-2.11_spark-2.4.5` |
| `7.3` - `7.6` | `scala-2.12_spark-3.0.1` |
| `8.0` - `8.3` | `scala-2.12_spark-3.1.1` |

1. Use Maven to build the POM located at `sample/spark-sample-job/pom.xml` or run the following Docker command:

    Linux:

    ```bash
    docker run -it --rm -v `pwd`/sample/spark-sample-job:/spark-sample-job -v "$HOME/.m2":/root/.m2 -w /spark-sample-job maven:3.6.3-jdk-8 mvn install -P <maven-profile>
    ```

    Windows:

    ```bash
    docker run -it --rm -v %cd%/sample/spark-sample-job:/spark-sample-job -v "%USERPROFILE%/.m2":/root/.m2 -w /spark-sample-job maven:3.6.3-jdk-8 mvn install -P <maven-profile>
    ```

1. Navigate to your Databricks workspace and create a new job, as described [here](https://docs.azuredatabricks.net/user-guide/jobs.html#create-a-job).

1. In the job detail page, select **Set JAR**.

1. Upload the JAR file from `/src/spark-jobs/target/spark-jobs-1.0-SNAPSHOT.jar`.

1. For **Main class**, enter `com.microsoft.pnp.samplejob.StreamingQueryListenerSampleJob`.

When the job runs, you can view the application logs and metrics in your Log Analytics workspace. After you verify the metrics appear, stop the sample application job.

### Viewing the Sample Job's Logs in Log Analytics

After your sample job has run for a few minutes, you should be able to query for
these event types in Log Analytics:

```sh
SparkListenerEvent_CL
SparkLoggingEvent_CL
SparkMetric_CL
```

One example of querying logs is:

```sh
SparkLoggingEvent_CL | where logger_name_s contains "com.microsoft.pnp"
```

Another example of querying metrics:

```sh
SparkMetric_CL
| where name_s contains "executor.cpuTime"
| extend sname = split(name_s, ".")
| extend executor=strcat(sname[0], ".", sname[1])
| project TimeGenerated, cpuTime=count_d / 100000
```

> Note: For more details on how to use the saved search queries in [logAnalyticsDeploy.json](/perftools/deployment/loganalytics/logAnalyticsDeploy.json) to understand and troubleshoot performance, see [Observability patterns and metrics for performance tuning](https://docs.microsoft.com/azure/architecture/databricks-monitoring/databricks-observability).

## Filtering

The library is configurable to limit the volume of logs that are sent to each of the different Azure Monitor log types.  See [filtering](./docs/filtering.md) for more details.

## Debugging

If you encounter any issues with the init script, you can refer to the docs on [debugging](./docs/debugging.md).

## Contributing

See: [CONTRIBUTING.md](CONTRIBUTING.md)
