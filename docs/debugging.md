# Debugging

If you have any issues with the init script, you can debug this by clicking on
the `Logging` tab in the `Advanced Options` section of your cluster
configuration and add a path to save the logs to such as:

```sh
dbfs:/cluster-logs
```

Then, you can download the logs to your local system with:

```sh
databricks fs cp -r dbfs:/cluster-logs <local-system-path>
```
