package actio.datapipes.task

import actio.common.Data._
import actio.common.{Dom, Event, Observer, Task}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Queue}

class TaskEvent(val name: String) extends Task {

  private val _observer: ListBuffer[Observer[Dom]] = ListBuffer()
  private val buffer = Queue[DataSet]()
  private val sentEvents = mutable.Set[Event]()

  def completed(): Unit = {

    val send =
      for {
      e <- buffer.grouped(100)
      o <- _observer
    } yield (e,o)

    send.foreach(s => s._2.next(Dom() ~ Dom("events", Nil, DataArray(s._1.toList), DataNothing(), Nil)))

    buffer.clear()
    sentEvents.clear()

    _observer.foreach(o => o.completed())
  }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {
    val currentEvents = value.children.flatMap(_.events) ::: value.events

    currentEvents.foreach {e =>
      if(sentEvents.add(e))
        buffer += eventToDataSet(e)
    }
  }

  def subscribe(observer: Observer[Dom]): Unit = {
    _observer.append(observer)
  }

  def eventToDataSet(e: Event): DataSet = {
      DataRecord(
        "event",
        DataString("pipeline_run_id", e.pipeInstanceId),
        DataString("task_run_id", e.taskInstanceId),
        DataString("event_type", e.theType),
        DataDate("event_time", e.time),
        DataString("action_type", e.theAction),
        DataString("keyName", e.keyName),
        DataString("message", e.msg),
        DataNumeric("counter_value", e.theCount),
        DataString("counter_label", e.counter)
      )
  }

}
