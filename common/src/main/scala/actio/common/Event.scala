package actio.common

import java.util.UUID

import actio.common.Data._
import java.text.SimpleDateFormat
import java.util.TimeZone

case class Event(
  pipeInstanceId: String,
  taskInstanceId: String,
  theType: String,
  theAction: String,
  msg: String,
  time: Long,
  keyName: String = "",
  counter: String = "",
  theCount: Int = 0
)

object Event {
  def apply(
    pipeInstanceId: String,
    taskInstanceId: String,
    theType: String,
    theAction: String,
    msg: String
  ) =
    new Event(pipeInstanceId, taskInstanceId, theType, theAction, msg, System.currentTimeMillis())

  import java.text.SimpleDateFormat

  val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  sdf.setTimeZone(TimeZone.getTimeZone("UTC"))

  def toDataSet(e: Event): DataSet = {
    DataRecord(
      "event",
      DataString("event_id", UUID.randomUUID().toString),
      DataString("pipeline_run_id", e.pipeInstanceId),
      DataString("task_run_id", e.taskInstanceId),
      DataString("detail", e.taskInstanceId),
      DataString("event_type", e.theType),
      DataString("event_time", sdf.format(new java.util.Date(e.time))),
      DataString("action_type", e.theAction),
      DataString("message", e.msg),
      DataString("counterValue", e.theCount.toString),
      DataString("counterTotal", e.theCount.toString),
      DataString("counterLabel", e.counter)
    )
  }

  def taskTotalError(
    pipeInstanceId: String,
    taskInstanceId: String,
    size: Int
  ) = new Event(pipeInstanceId, taskInstanceId,
    "TASK FINISH", "PARTIAL", "Some data for this Task failed to process.", System.currentTimeMillis(), "Total Error", "Total Error", size)

  def taskTotalSizeError(
    pipeInstanceId: String,
    taskInstanceId: String,
    size: Int
  ) = new Event(pipeInstanceId, taskInstanceId,
    "TASK FINISH", "PARTIAL", "Some data for this Task failed to process.", System.currentTimeMillis(), "Total Size Error", "Total Size Error", size)

  def taskNoErrorTotal(pipeInstanceId: String,
                  taskInstanceId: String,
                  size: Int
                 ) = new Event(pipeInstanceId, taskInstanceId,
    "TASK FINISH", "COMPLETE", "All data has been processed successfully.", System.currentTimeMillis(), "Total Processed", "Total Processed", size)

  def taskNoErrorTotalSize(pipeInstanceId: String,
                       taskInstanceId: String,
                       size: Int
                      ) = new Event(pipeInstanceId, taskInstanceId,
    "TASK FINISH", "COMPLETE", "All data has been processed successfully.", System.currentTimeMillis(), "Total Size Processed", "Total Size Processed", size)

}