package actio.common

import java.util.UUID

import actio.common.Data._
import java.text.SimpleDateFormat
import java.util.TimeZone

trait Event {
  def pipeInstanceId: String
  def taskInstanceId: String
  def eventType: String
  def message: String
  def time: Long
}

case class EventMetric(
  pipeInstanceId: String,
  taskInstanceId: String,
  eventType: String,
  message: String,
  time: Long,
  totalError: Int = 0,
  totalErrorSize: Int = 0,
  totalProcessed: Int = 0,
  totalProcessedSize: Int = 0
) extends Event

case class EventAssertionFailed(
  pipeInstanceId: String,
  taskInstanceId: String,
  eventType: String,
  message: String,
  time: Long,
  abort: Boolean,
  statusCode: Int
) extends Event

object Event {
  def apply(
    pipeInstanceId: String,
    taskInstanceId: String,
    eventType: String,
    message: String
  ) = new EventMetric(pipeInstanceId, taskInstanceId, eventType, message, System.currentTimeMillis())

  import java.text.SimpleDateFormat

  val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  sdf.setTimeZone(TimeZone.getTimeZone("UTC"))

  def toDataSet(e: Event): DataSet = e match {
    case em: EventMetric =>
      DataRecord(
        "event",
        DataString("event_id", UUID.randomUUID().toString),
        DataString("pipeline_run_id", e.pipeInstanceId),
        DataString("task_run_id", e.taskInstanceId),
        DataString("event_type", e.eventType),
        DataString("event_time", sdf.format(new java.util.Date(e.time))),
        DataString("message", e.message),
        DataNumeric("totalError", em.totalError),
        DataNumeric("totalErrorSize", em.totalErrorSize),
        DataNumeric("totalProcessed", em.totalProcessed),
        DataNumeric("totalProcessedSize", em.totalProcessedSize)
      )
    case _ => {
      DataRecord(
        "event",
        DataString("event_id", UUID.randomUUID().toString),
        DataString("pipeline_run_id", e.pipeInstanceId),
        DataString("task_run_id", e.taskInstanceId),
        DataString("event_type", e.eventType),
        DataString("event_time", sdf.format(new java.util.Date(e.time))),
        DataString("message", e.message)
      )
    }
  }

  def taskCompleted(
    pipeInstanceId: String,
    taskInstanceId: String,
    totalError: Int,
    totalErrorSize: Int,
    totalProcessed: Int,
    totalProcessedSize: Int,
    message: String
  ) = new EventMetric(pipeInstanceId, taskInstanceId, "Task Completed", "Task " + taskInstanceId + "completed.", System.currentTimeMillis(),
    totalError, totalErrorSize, totalProcessed, totalProcessedSize)

  def taskError(
    pipeInstanceId: String,
    taskInstanceId: String,
    totalError: Int,
    totalErrorSize: Int,
    totalProcessed: Int,
    totalProcessedSize: Int,
    exception: Exception
  ) = new EventMetric(pipeInstanceId, taskInstanceId, "Task Error", exception.getMessage, System.currentTimeMillis(),
    totalError, totalErrorSize, totalProcessed, totalProcessedSize)

  def runStarted() = new EventMetric("", "", "Run Started", "Run Started.", System.currentTimeMillis(),
    0, 0, 0, 0)

  def runCompleted() = new EventMetric("", "", "Run Completed", "Run Completed.", System.currentTimeMillis(),
    0, 0, 0, 0)

  def taskExit(
    pipeInstanceId: String,
    taskInstanceId: String,
    message: String,
    abort: Boolean,
    statusCode: Int
  ) = new EventAssertionFailed(pipeInstanceId, taskInstanceId, "Task Validation", message, System.currentTimeMillis(), abort, statusCode)

}