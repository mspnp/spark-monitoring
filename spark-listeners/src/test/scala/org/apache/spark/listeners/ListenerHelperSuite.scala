package org.apache.spark.listeners

import org.apache.spark.internal.Logging
import org.apache.spark.{LogAnalytics, SparkFunSuite}
import org.json4s.JsonAST
import org.json4s.jackson.JsonMethods.compact

class ListenerHelperSuite extends SparkFunSuite {

  //  trait that overrides concrete LogAnalytics trait which is wired to sends event to real endpoint
  //  when traits are extended, if same method names occur, always the method of the last
  //  trait is invoked in the instantiated object
  trait LogAnalyticsMock extends LogAnalytics {

    this: Logging =>

    var isLogEventInvoked = false

    var enrichedLogEvent = ""

    override protected[spark] def logEvent(json: Option[JsonAST.JValue]): Unit = {
      println("log Event got invoked")
      isLogEventInvoked = !isLogEventInvoked

      if (json.isDefined) {
        enrichedLogEvent = compact(json.get)
        println(enrichedLogEvent)
      }

    }


  }

}
