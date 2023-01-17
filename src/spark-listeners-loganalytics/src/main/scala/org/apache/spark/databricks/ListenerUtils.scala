package org.apache.spark.databricks

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.microsoft.pnp.LogAnalyticsEnvironment
import com.microsoft.pnp.client.loganalytics.{LogAnalyticsClient, LogAnalyticsSendBufferClient}
import org.apache.spark.com.microsoft.pnp.SparkInformation
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.streaming.scheduler.StreamingListenerEvent

import java.time.Instant

object ListenerUtils {

  private val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  private val workspaceId = LogAnalyticsEnvironment.getWorkspaceId
  private val secret = LogAnalyticsEnvironment.getWorkspaceKey
  private val logType = "SparkMetricTest"

  private val client = new LogAnalyticsSendBufferClient(new LogAnalyticsClient(workspaceId, secret), logType)

  def sendListenerEventToLA(event: SparkListenerEvent): Unit = {
    val eventAsString = parse(objectMapper.convertValue(event, classOf[Map[String, String]]))
    client.sendMessage(eventAsString, "SparkEventTime")
  }

  def sendStreamingEventToLA(event: StreamingListenerEvent): Unit = {
    val eventAsString = parse(objectMapper.convertValue(event, classOf[Map[String, String]]))
    client.sendMessage(eventAsString, "SparkEventTime")
  }

  private def parse(eventAsMap: Map[String, String]) : String = {
    val enrichedEvent = eventAsMap ++
      SparkInformation.get() +
      ("SparkEventTime" -> Instant.now().toString)

    objectMapper.writeValueAsString(enrichedEvent)
  }

}
