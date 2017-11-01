package actio.common

import java.util.UUID

import actio.common.Data._
import java.text.SimpleDateFormat
import java.util.TimeZone

case class Event(
  pipeInstanceId: String,
  taskInstanceId: String,
  eventType: String,
  message: String,
  time: Long,
  totalError: Int = 0,
  totalErrorSize: Int = 0,
  totalProcessed: Int = 0,
  totalProcessedSize: Int = 0
)

object Event {
  def apply(
    pipeInstanceId: String,
    taskInstanceId: String,
    eventType: String,
    message: String
  ) =
    new Event(pipeInstanceId, taskInstanceId, eventType, message, System.currentTimeMillis())

  import java.text.SimpleDateFormat

  val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  sdf.setTimeZone(TimeZone.getTimeZone("UTC"))

  def toDataSet(e: Event): DataSet = {
    DataRecord(
      "event",
      DataString("event_id", UUID.randomUUID().toString),
      DataString("pipeline_run_id", e.pipeInstanceId),
      DataString("task_run_id", e.taskInstanceId),
      DataString("event_type", e.eventType),
      DataString("event_time", sdf.format(new java.util.Date(e.time))),
      DataString("message", e.message),
      DataNumeric("totalError", e.totalError),
      DataNumeric("totalErrorSize", e.totalErrorSize),
      DataNumeric("totalProcessed", e.totalProcessed),
      DataNumeric("totalProcessedSize", e.totalProcessedSize)
    )
  }

  def taskCompleted(
    pipeInstanceId: String,
    taskInstanceId: String,
    totalError: Int,
    totalErrorSize: Int,
    totalProcessed: Int,
    totalProcessedSize: Int,
    message: String
  ) = new Event(pipeInstanceId, taskInstanceId, "Task Completed", "Task " + taskInstanceId + "completed.", System.currentTimeMillis(),
    totalError, totalErrorSize, totalProcessed, totalProcessedSize)

  def taskError(pipeInstanceId: String,
                taskInstanceId: String,
                totalError: Int,
                totalErrorSize: Int,
                totalProcessed: Int,
                totalProcessedSize: Int,
                exception: Exception
               ) = new Event(pipeInstanceId, taskInstanceId, "Task Error", exception.getMessage, System.currentTimeMillis(),
    totalError, totalErrorSize, totalProcessed, totalProcessedSize)

  def runStarted() =  new Event("", "", "Run Started", "Run Started.", System.currentTimeMillis(),
    0, 0, 0, 0)

  def runCompleted() = new Event("", "", "Run Completed", "Run Completed.", System.currentTimeMillis(),
    0, 0, 0, 0)

}