package com.microsoft.pnp

import org.apache.spark.listeners.LogAnalyticsStreamingQueryListener
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

object StreamingQueryListenerSampleJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .getOrCreate

    import spark.implicits._

    spark.streams.addListener(new LogAnalyticsStreamingQueryListener(spark.sparkContext.getConf))

    // this path has sample files provided by databricks for trying out purpose
    val inputPath = "/databricks-datasets/structured-streaming/events/"

    val jsonSchema = new StructType().add("time", TimestampType).add("action", StringType)

    // Similar to definition of staticInputDF above, just using `readStream` instead of `read`
    val streamingInputDF =
      spark
        .readStream // `readStream` instead of `read` for creating streaming DataFrame
        .schema(jsonSchema) // Set the schema of the JSON data
        .option("maxFilesPerTrigger", 1) // Treat a sequence of files as a stream by picking one file at a time
        .json(inputPath)

    val streamingCountsDF =
      streamingInputDF
        .groupBy($"action", window($"time", "1 hour"))
        .count()

    // Is this DF actually a streaming DF?
    streamingCountsDF.isStreaming

    val query =
      streamingCountsDF
        .writeStream
        .format("memory") // memory = store in-memory table (for testing only in Spark 2.0)
        .queryName("counts") // counts = name of the in-memory table
        .outputMode("complete") // complete = all the counts should be in the table
        .start()
  }
}
