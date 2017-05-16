package Task

import DataPipes.Common._
import DataPipes.Common.Data._

abstract class TaskTransform(val name: String) extends Task {

  var _observer: Option[Observer[Dom]] = None

  def completed(): Unit = {
    if(_observer.isDefined)
      { _observer.get.completed() }
  }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {

    val nds = transform(value)

    if(_observer.isDefined)
      { _observer.get.next(value ~ Dom(name,null,List(),nds,DataNothing())) }
  }

  def subscribe(observer: Observer[Dom]): Unit = {
    _observer = Some(observer)
  }

  def transform(dom: Dom): DataSet

}