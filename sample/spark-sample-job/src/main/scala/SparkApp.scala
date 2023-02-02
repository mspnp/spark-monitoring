import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkApp {

    def main(args: Array[String]) {

      val spark = SparkSession.builder.appName("Simple Application")
        .config("spark.extraListeners","com.microsoft.pnp.listeners.DatabricksListener")
        .master("local").getOrCreate()
      import spark.implicits._

      LogManager.getLogger.warn("Test message from local application")

      val someDF = Seq(
        (8, "bat"),
        (64, "mouse"),
        (-27, "horse")
      ).toDF("number", "word")

      val frame = someDF.groupBy("number").agg(min("number"))
      println(frame.count())

      someDF.show()
      spark.stop()
    }

}
