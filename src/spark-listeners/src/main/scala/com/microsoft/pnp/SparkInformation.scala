package com.microsoft.pnp

import org.apache.spark.SparkEnv

object SparkInformation {
  // Spark Configuration
  // NOTE - In Spark versions > 2.4.0, many settings have been, or will likely be, replaced with values
  // in the internal config package, so these should be replaced with those.
  private val EXECUTOR_ID = "spark.executor.id"
  private val APPLICATION_ID = "spark.app.id"
  private val APPLICATION_NAME = "spark.app.name"

  // Databricks-specific
  private val DB_CLUSTER_ID = "spark.databricks.clusterUsageTags.clusterId"
  private val DB_CLUSTER_NAME = "spark.databricks.clusterUsageTags.clusterName"
  // This is the environment variable name set in our init script.
  private val DB_CLUSTER_ID_ENVIRONMENT_VARIABLE = "DB_CLUSTER_ID"
  private val DB_CLUSTER_NAME_ENVIRONMENT_VARIABLE = "DB_CLUSTER_NAME"

  def get(): Map[String, String] = {
    // We might want to improve this to pull valid class names from the beginning of the command
    val className = "^(\\S*).*$".r
    // The sun.java.command is valid on the Oracle and OpenJDK JVMs, so we should be okay
    // for Databricks
    val nodeType = System.getProperty("sun.java.command") match {
      // Most of these values come from the org/apache/spark/launcher/SparkClassCommandBuilder.java
      // file.  When Spark is upgraded to new versions, this file needs to be checked.
      // We are basically taking the first part of the command passed to the
      // ${SPARK_HOME}/bin/spark-class script.  We have to do this because we cannot
      // always get to a SparkEnv.  If we don't have a match on any of these, we will
      // at least return the full class name.
      case className(c) => Some(c match {
        case "org.apache.spark.deploy.master.Master" => "master"
        case "org.apache.spark.deploy.worker.Worker" => "worker"
        case "org.apache.spark.executor.CoarseGrainedExecutorBackend" => "executor"
        case "org.apache.spark.deploy.ExternalShuffleService" => "shuffle"
        // The first value is returned because we on Databricks running a JAR job or
        // a Notebook, since Databricks wraps up the Spark stuff
        case "com.databricks.backend.daemon.driver.DriverDaemon" |
             "org.apache.spark.deploy.SparkSubmit" => "driver"
        case _ => c
      })
      case _ => None
    }

    val sparkInfo = Option(SparkEnv.get) match {
      case Some(e) => {
        val conf = e.conf
        Map(
          "applicationId" -> conf.getOption(APPLICATION_ID),
          "applicationName" -> conf.getOption(APPLICATION_NAME),
          "clusterId" -> conf.getOption(DB_CLUSTER_ID),
          "clusterName" -> conf.getOption(DB_CLUSTER_NAME),
          "executorId" -> Option(e.executorId),
          "nodeType" -> nodeType
        )
      }
      case None => {
        // If we don't have a SparkEnv, we could be on any node type, really.
        Map(
          "clusterId" -> sys.env.get(DB_CLUSTER_ID_ENVIRONMENT_VARIABLE),
          "clusterName" -> sys.env.get(DB_CLUSTER_NAME_ENVIRONMENT_VARIABLE),
          "nodeType" -> nodeType
        )
      }
    }

    // We will remove None values and convert to Map[String, String] to make conversion
    // less painful.
    for ((k, Some(v)) <- sparkInfo ) yield k -> v
  }
}
