import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.Row

import java.lang.Thread.sleep

object SparkApp {
  import org.apache.spark.sql.SparkSession

    def main(args: Array[String]) {

      val spark = SparkSession.builder.appName("Simple Application")
        .config("spark.extraListeners","com.microsoft.pnp.listeners.UltimateListener")
        .master("local").getOrCreate()
      import spark.implicits._

      LogManager.getLogger.warn("Test message from local application")

      val someDF = Seq(
        (8, "bat"),
        (64, "mouse"),
        (-27, "horse")
      ).toDF("number", "word")

      someDF.show()
      sleep(10000)
      spark.stop()
    }

}
