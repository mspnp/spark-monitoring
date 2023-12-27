# Spark Performance Monitoring With Grafana and Log Analytics

## Deployment of Log Analytics With Spark Metrics

For more details on how to use Grafana to monitor Spark performance, visit: [Use dashboards to visualize Azure Databricks metrics](https://learn.microsoft.com/azure/architecture/databricks-monitoring/dashboards).

### Step 1: Deploy Log Analytics With Spark Metrics

Open an Azure bash cloud shell or a bash command shell and execute the azure cli command,  Replacing yourResourceGroupName and yourLocation.

```
export RGNAME=yourResourceGroupName
# location example "East Us"
export RGLOCATION=yourLocation

az group create --name "${RGNAME}" --location "${RGLOCATION}"

az group deployment create --resource-group $RGNAME \
--template-uri https://raw.githubusercontent.com/mspnp/spark-monitoring/master/perftools/deployment/loganalytics/logAnalyticsDeploy.json \
--parameters location="${RGLOCATION}" \
 dataRetention=30 serviceTier=PerGB2018
```
if you run the command *az group deployment create --resource-group $RGNAME --template-uri https://raw.githubusercontent.com/mspnp/spark-monitoring/master/perftools/deployment/loganalytics/logAnalyticsDeploy.json* it will prompt for all required parameters



## Deployment of Grafana

### Step 1: Deploy Certified Grafana From Azure
For Grafana deployment a bitnami certified image will be used. You can find more information about bitnami applications on azure at https://docs.bitnami.com/azure/get-started-marketplace/

1. Open an Azure bash cloud shell or a bash command shell and execute the below command and azure cli, note the VM password has minimum requirements of 12 characters and be a strong password. Replace yourResourceGroupName and yourLocation.

```
read -s VMADMINPASSWORD
```

```
export RGNAME=yourResourceGroupName
# location example "South Central US"
export RGLOCATION=yourLocation

az group create --name "${RGNAME}" --location "${RGLOCATION}"

az group deployment create --resource-group $RGNAME \
--template-uri https://raw.githubusercontent.com/mspnp/spark-monitoring/master/perftools/deployment/grafana/grafanaDeploy.json \
--parameters adminPass=$VMADMINPASSWORD \
 dataSource=https://raw.githubusercontent.com/mspnp/spark-monitoring/master/perftools/deployment/grafana/AzureDataSource.sh
```

if you run the command *az group deployment create --resource-group $RGNAME --template-uri https://raw.githubusercontent.com/mspnp/spark-monitoring/master/perftools/deployment/grafana/grafanaDeploy.json* it will prompt for all required parameters

After the ARM template deploys the bitnami image of grafana a temporary grafana password for user admin will be created. To find those credentials follow bellow instructions.

2.  You can either click on grafana vm resource then click on **Boot diagnostics** then click on **serial log**. Search for the string **Setting Bitnami application password to**, or you can open ssh connection to the grafana vm and enter the command **cat ./bitnami_credentials** you can find instructions by following  https://docs.bitnami.com/azure/faq/get-started/find-credentials/.

### Step 2: Change Grafana Administrator Password
1. Open the browser at http://grafanapublicipaddress:3000 and login in as admin and password from previous step
2. Move the mouse on the settings icon located to the left then click on **Server Admin**
![change user Logo](./images/UserChange.png)
3. Click on admin then on the text box **Change Password** enter new password the click **Update**

### Step 3: Create Service Principal for Azure Monitor Data Source Using Azure Cli

1. Enter command below to login to azure

```
az login
 ```
2. Make sure you are on the right subscription. You can set the default subscription with command below:
```
az account show
az account set --subscription yourSubscriptionId
```
3. Create the Service Principal running below command.


```
az ad sp create-for-rbac --name http://NameOfSp --role "Log Analytics Reader"
```
4. Take note of appId, password and tenant

```
{
  "appId": "applicationClientId",
  "displayName": "applicationName",
  "name": "http://applicationName",
  "password": "applicationSecret",
  "tenant": "TenantId"
}
```

### Step 4: Create Azure Monitor Datasource in Grafana


1. On grafana move mouse on the settings icon located to the left then click on **Data Sources** then **Add data Source**. Select **Azure Monitor**.
![change user Logo](./images/AddSource.png)

2. Enter **ALA** in name, SubscriptionId, TenantId(tenant in previous step), Client id(appId in previous step), Client secret (password in previous step). Click on check box **Same Details as Azure Monitor api** then click on **Save & test**
![change user Logo](./images/DataSource.png)

### Step 5: Import Spark Metrics Dashboard

1. Open a bash shell command prompt, move to the directory containing SparkMetricsDashboardTemplate.json file and execute commands below, replacing YOUR_WORKSPACEID with the workspace id of Log Analytics and SparkListenerEvent_CL with the log type if a non default logtype is used for spark monitoring.
The workspace id for Log Analytics can be found at the **advanced settings** blade of Log Analytics resource.


```
export WORKSPACE=YOUR_WORKSPACEID
export LOGTYPE=SparkListenerEvent_CL

sed "s/YOUR_WORKSPACEID/${WORKSPACE}/g" "SparkMetricsDashboardTemplate.json"  | sed  "s/SparkListenerEvent_CL/${LOGTYPE}/g"    > SparkMetricsDash.json
```

or execute below script from same directory location

```
export WORKSPACE=YOUR_WORKSPACEID
export LOGTYPE=SparkListenerEvent_CL

sh DashGen.sh
```

2. On grafana move mouse on the settings icon located to the left then click on **Manage** then **Import**, browse to directory /spark-monitoring/perftools/dashboards/grafana click on  open SparkMetricsDash.json. Then select your azure monitor data source that was create before
![change user Logo](./images/Import.png)
