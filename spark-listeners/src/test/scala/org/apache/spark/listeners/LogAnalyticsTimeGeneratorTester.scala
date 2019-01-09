package org.apache.spark.listeners

import org.scalatest.FunSuite

class LogAnalyticsTimeGeneratorTester extends FunSuite {


  test("should get time in ISO 8601 format YYYY-MM-DDThh:mm:ssZ") {

    val time: Long = 1547012862
    val expected_ISO8601_Time = "2019-01-09T05:47:42Z"
    val actual_ISO8601_Time = LogAnalyticsTimeGenerator
      .getTime(time)
    assert(expected_ISO8601_Time.contentEquals(actual_ISO8601_Time))

  }

  test("should get currentTime in ISO 8601 format YYYY-MM-DDThh:mm:ssZ") {
    val actual_ISO8601_Time = LogAnalyticsTimeGenerator
      .getTime()
    assert(actual_ISO8601_Time.length != 0)
  }

}
