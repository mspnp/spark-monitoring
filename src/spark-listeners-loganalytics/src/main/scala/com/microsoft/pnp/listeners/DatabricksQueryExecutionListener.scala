package com.microsoft.pnp.listeners

import com.microsoft.pnp.listeners.ListenerUtils.{QueryExecutionDuration, QueryExecutionException}
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class DatabricksQueryExecutionListener extends QueryExecutionListener {
  private val LOGGER = LogManager.getLogger();

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = processStreamingEvent(funcName, qe, durationNs)

  private def processStreamingEvent(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    try {
      ListenerUtils.sendQueryEventToLA(new QueryExecutionDuration(funcName, qe.toString(), durationNs))
    } catch {
      case e: Exception =>
        LOGGER.error("Could not parse event " + qe.getClass.getName)
        LOGGER.error(e.getMessage)
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = processStreamingEvent(funcName, qe, exception)

  private def processStreamingEvent(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    try {
      ListenerUtils.sendQueryEventToLA(new QueryExecutionException(funcName, qe.toString(), exception))
    } catch {
      case e: Exception =>
        LOGGER.error("Could not parse event " + qe.getClass.getName)
        LOGGER.error(e.getMessage)
    }
  }
}
