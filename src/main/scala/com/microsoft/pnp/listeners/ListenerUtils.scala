package com.microsoft.pnp.listeners

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.microsoft.pnp.LogAnalyticsEnvironment
import com.microsoft.pnp.client.loganalytics.{LogAnalyticsClient, LogAnalyticsSendBufferClient}
import com.microsoft.pnp.loganalytics.SparkInformation
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.streaming.scheduler.StreamingListenerEvent

import java.time.Instant

object ListenerUtils {
  private val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val workspaceId = LogAnalyticsEnvironment.getWorkspaceId
  private val secret = LogAnalyticsEnvironment.getWorkspaceKey
  private val logType = "SparkListenerEvent"
  private val client = new LogAnalyticsSendBufferClient(new LogAnalyticsClient(workspaceId, secret), logType)

  def sendStreamingQueryEventToLA(event: StreamingQueryListener.Event): Unit = {
    val eventAsString = parse(objectMapper.convertValue(event, classOf[Map[String, String]]))
    sendEvent(eventAsString)
  }

  def sendQueryEventToLA(qe: QueryExecutionDuration): Unit = {
    val eventAsString = parse(objectMapper.convertValue(qe, classOf[Map[String, String]]))
    sendEvent(eventAsString)
  }

  def sendQueryEventToLA(qe: QueryExecutionException): Unit = {
    val eventAsString = parse(objectMapper.convertValue(qe, classOf[Map[String, String]]))
    sendEvent(eventAsString)
  }

  def parse(eventAsMap: Map[String, String]): String = {
    val enrichedEvent = eventAsMap ++
      SparkInformation.get() +
      ("SparkEventTime" -> Instant.now().toString)

    objectMapper.writeValueAsString(enrichedEvent)
  }

  def sendListenerEventToLA(event: SparkListenerEvent): Unit = {
    val eventAsString = parse(objectMapper.convertValue(event, classOf[Map[String, String]]))
    sendEvent(eventAsString)
  }

  def sendStreamingEventToLA(event: StreamingListenerEvent): Unit = {
    val eventAsString = parse(objectMapper.convertValue(event, classOf[Map[String, String]]))
    sendEvent(eventAsString)
  }

  def sendEvent(eventAsString: String): Unit = {
    client.sendMessage(eventAsString, "SparkEventTime")
  }

  case class QueryExecutionDuration(funcName: String, qe: String, val durationNs: Long)

  case class QueryExecutionException(val funcName: String, val qe: String, val exception: Exception)

}
