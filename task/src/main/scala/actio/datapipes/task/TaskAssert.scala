package actio.datapipes.task

import actio.common.Data.{DataBoolean, DataNothing, DataSet}
import actio.common.{Dom, Event, Observer}
import actio.datapipes.task.Term.TermExecutor

import scala.collection.mutable.ListBuffer
import scala.meta._

class TaskAssert(name: String, config: DataSet) extends actio.common.Task {
  val _observer: ListBuffer[Observer[Dom]] = ListBuffer()
  val term: Term = config("term").toString.parse[Term].get
  val executor = new TermExecutor(config("namespace").stringOption.getOrElse("actio.datapipes.task.Term.Legacy.Functions"))
  val message: String = config("message").toString
  val abort: Boolean = config("abort").stringOption.contains("true")
  val statusCode: Int = config("statuscode").intOption.getOrElse(1)

  def completed(): Unit = _observer.foreach(o => o.completed())

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {
    val events = value.success.flatMap(v => executor.eval(v, term) match {
      case DataBoolean(_, false) => List(Event.taskExit("", this.name, message, abort, statusCode))
      case _ => Nil
    }).toList

    _observer.foreach(o => o.next(Dom(name, List(), value.success, DataNothing(), events)))
  }

  def subscribe(observer: Observer[Dom]): Unit = _observer.append(observer)
}
