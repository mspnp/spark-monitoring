import com.microsoft.pnp.listeners.{DatabricksQueryExecutionListener, DatabricksStreamingListener, DatabricksStreamingQueryListener}
import org.apache.logging.log4j.LogManager
import org.apache.spark.streaming.StreamingContext

import java.lang.Thread.sleep

object SparkAppStream {
  import org.apache.spark.sql.SparkSession

    def main(args: Array[String]) {

      val spark = SparkSession.builder.appName("Simple Application")
       // .config("spark.extraListeners","com.microsoft.pnp.listeners.UltimateListener")
        .master("local").getOrCreate()
      import spark.implicits._

      LogManager.getLogger.warn("Test message from local application")

      spark.streams.addListener(new DatabricksStreamingQueryListener())
      spark.listenerManager.register(new DatabricksQueryExecutionListener())



      val df = spark.readStream.format("rate").option("rowsPerSecond", 10).option("rampUpTime", 10).load()

      val query1 = df
        .writeStream
        .format("console")
        .start()

      df.printSchema()
      //StreamingContext.getActive().get.addStreamingListener(new DatabricksStreamingListener())

      query1.awaitTermination()
      sleep(10000)
      spark.stop()
    }

}
