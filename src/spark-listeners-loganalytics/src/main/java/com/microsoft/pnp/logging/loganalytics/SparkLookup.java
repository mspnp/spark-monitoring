package com.microsoft.pnp.logging.loganalytics;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.lookup.StrLookup;
import org.apache.spark.SparkEnv;

@Plugin(name = "spark", category = StrLookup.CATEGORY)
public class SparkLookup implements StrLookup {

    @Override
    public String lookup(String key) {
        return getFromSpark(key);
    }

    @Override
    public String lookup(LogEvent event, String key) {
        return getFromSpark(key);
    }

    private String getFromSpark(String key) {

        if ("nodeType".equals(key)) {
            String nodeType = System.getProperty("sun.java.command");
            if (nodeType == null) {
                return null;
            }
            switch (nodeType) {
                case "org.apache.spark.deploy.master.Master":
                    return "master";
                case "org.apache.spark.deploy.worker.Worker":
                    return "worker";
                case "org.apache.spark.executor.CoarseGrainedExecutorBackend":
                    return "executor";
                case "org.apache.spark.deploy.ExternalShuffleService":
                    return "shuffle";
                // The first value is returned because we on Databricks running a JAR job or
                // a Notebook, since Databricks wraps up the Spark stuff
                case "com.databricks.backend.daemon.driver.DriverDaemon":
                    return "driver";
                case "org.apache.spark.deploy.SparkSubmit":
                    return "driver";
                case "SparkApp":
                    return "driver";
                default:
                    return null;
            }
        }
        SparkEnv sparkEnv = SparkEnv.get();
        if (sparkEnv == null) return null;
        return sparkEnv.conf().get(key, null);
    }
}