package actio.datapipes.task

import java.util.UUID

import actio.common.Data._
import actio.common.{Dom, Event, Observer, Task}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Queue}

class TaskEvent(val name: String) extends Task {

  private val _observer: ListBuffer[Observer[Dom]] = ListBuffer()
  private val sendEvents = mutable.LinkedHashSet[Event]()

  def completed(): Unit = {

    // send the finish event after sending all events
    val endEvent = Event(sendEvents.headOption.map(_.pipeInstanceId).getOrElse(""),"", "INFO", "FINISH", "Ending DataPipes Runtime")
    sendEvents += endEvent

    //val send =
    //  for {
    //  e <- sendEvents.map(eventToDataSet).grouped(100)
    //  o <- _observer
    //} yield (e,o)

    //send.foreach(s => s._2.next(Dom("events", Nil, DataArray(s._1.toList), DataNothing(), Nil)))

    sendEvents.clear()

    _observer.foreach(o => o.completed())
  }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {
    sendEvents ++= value.children.flatMap(_.events) ::: value.events
  }

  def subscribe(observer: Observer[Dom]): Unit = {
    _observer.append(observer)
  }



}
