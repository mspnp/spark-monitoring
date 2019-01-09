package org.apache.spark.listeners

import org.apache.spark.SparkFunSuite
import org.apache.spark.scheduler.SparkListenerApplicationStart

class LamdaTester extends SparkFunSuite {

  test("lambda should be able to interpret anref to actualevent") {

    val expectedAppName = "someAppName"
    val mockSparkListAppStartEvent = SparkListenerApplicationStart(
      expectedAppName,
      Option.empty[String],
      1L,
      "someSparkUser",
      Option.empty[String]
    )
    val mockLambda: AnyRef => String = (o: AnyRef) => {

      val actualSparkListAppStartEvent = o.asInstanceOf[SparkListenerApplicationStart]
      actualSparkListAppStartEvent.appName
    }

    val actualAppName = mockLambda.apply(mockSparkListAppStartEvent)

    assert(actualAppName.contentEquals(expectedAppName))
  }


}
