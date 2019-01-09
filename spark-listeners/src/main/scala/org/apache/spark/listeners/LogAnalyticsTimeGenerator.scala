package org.apache.spark.listeners

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}

object LogAnalyticsTimeGenerator {


  def getTime(time: Long): String = {

    val zdt = ZonedDateTime.ofInstant(Instant.ofEpochSecond(time), ZoneId.systemDefault)
    zdt.format(DateTimeFormatter.ISO_INSTANT)

  }

  def getTime(): String = {

    val zdt = ZonedDateTime.ofInstant(Instant.ofEpochSecond(System.currentTimeMillis()/1000), ZoneId.systemDefault)
    zdt.format(DateTimeFormatter.ISO_INSTANT)
  }

}
