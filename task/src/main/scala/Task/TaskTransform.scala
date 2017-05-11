package Task

import DataPipes.Common._
import DataPipes.Common.Data._

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

abstract class TaskTransform(val name: String) extends Task {

  var _observer: Option[Observer[Dom]] = None

  def completed(): Future[Unit] = async {
    if(_observer.isDefined)
      await { _observer.get.completed() }
  }

  def error(exception: Throwable): Future[Unit] = ???

  def next(value: Dom): Future[Unit] = async {

    val nds = transform(value)

    if(_observer.isDefined)
      await { _observer.get.next(value ~ Dom(name,null,List(),nds,DataNothing())) }
  }

  def subscribe(observer: Observer[Dom]): Unit = {
    _observer = Some(observer)
  }

  def transform(dom: Dom): DataSet

}