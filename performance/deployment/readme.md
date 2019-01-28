# Deployment of Grafana dashboard and Log analytics workspace

## Deployment of Grafana

### Step 1: Deploy Certified Grafana From Azure
For Grafana deployment a bitnami certified image will be used. You can find more information about bitnami applications on azure at https://docs.bitnami.com/azure/get-started-marketplace/


1. Open project located at spark-monitoring\performance\deployment\
grafana\
GrafanaResource\GrafanaResource.sln, right click on the project **Grafana Resource** then select **deploy**.
2. click on  **Edit Parameters** and enter the admin password for the linux VM on **adminPass** parameter. Click **deploy**.

After the ARM template deploys the bitnami image of grafana a temporary grafana password for user admin will be created. To find those credentials follow bellow instructions.

3. Find the credentials for grafana by following instruction on https://docs.bitnami.com/azure/faq/get-started/find-credentials/

### Step 2: Change Grafana Administrator Password
1. Open the browser at http://grafanapublicipaddress:3000 and login in as admin and password from previous step
2. Move the mouse on the settings icon located to the left then click on **Server Admin**
![change user Logo](.\images\UserChange
  .png)
3. Click on admin then on the text box **Change Password** enter new password the click **Update**

### Step 3: Create Service Principal for Azure Monitor Data Source
  
