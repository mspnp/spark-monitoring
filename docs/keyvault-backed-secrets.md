# Setting Up an Azure Key Vault-Backed Secret Scope

To set up your databricks cluster to reference Key Vault-backed secrets,
follow the steps below:

1. Set up an [Azure Key Vault-backed secret scope](https://learn.microsoft.com/azure/databricks/security/secrets/secret-scopes#--create-an-azure-key-vault-backed-secret-scope).
1. Reference your Key Vault-backed keys in your cluster's environment variables:

```sh
LOG_ANALYTICS_WORKSPACE_ID={{secrets/<secret-scope-name>/<keyvault-key-name>}}
LOG_ANALYTICS_WORKSPACE_KEY={{secrets/<secret-scope-name>/<keyvault-key-name>}}
```

1. Edit the spark-monitoring.sh initialization script to reference these values:

```sh
LOG_ANALYTICS_WORKSPACE_ID=$LOG_ANALYTICS_WORKSPACE_ID
LOG_ANALYTICS_WORKSPACE_KEY=$LOG_ANALYTICS_WORKSPACE_KEY
```
