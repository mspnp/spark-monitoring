package org.apache.spark.listeners

import org.apache.spark.{LogAnalytics, SparkFunSuite}

class ListenerHelperSuite extends SparkFunSuite {

  //  trait that overrides concrete LogAnalytics trait which is wired to sends event to real endpoint
  //  when traits are extended, if same method names occur, always the method of the last
  //  trait is invoked in the instantiated object
  trait LogAnalyticsMock {

    this: LogAnalytics =>

    var isLogEventInvoked = false

    override protected def logEvent(event: AnyRef): Unit = {
      println("Log event got invoked")
      isLogEventInvoked = !isLogEventInvoked
    }

  }
}
