package actio.common

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
}