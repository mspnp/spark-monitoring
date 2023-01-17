import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.Row

import java.lang.Thread.sleep

object SparkApp {
  import org.apache.spark.sql.SparkSession

    def main(args: Array[String]) {

      val spark = SparkSession.builder.appName("Simple Application")
        .config("spark.extraListeners","org.apache.spark.databricks.UltimateListener")
        .master("local").getOrCreate()
      import spark.implicits._

      LogManager.getLogger.warn("Test message from local application")

      val someDF = spark.read.csv("/Users/william.conti/Projects/spark-monitoring-fb/sample/spark-sample-job/testData.csv")
      someDF.show()
      sleep(10000)
      spark.stop()
    }

}
