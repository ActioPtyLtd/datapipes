package Task

import Common.Data.DataNothing
import Common.{Dom, Observer}
import Common.Data.PrettyPrint.PrettyPrint

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class TaskPrint(val name: String) extends Common.Task {
  var _observer: Option[Observer[Dom]] = None

  def completed(): Future[Unit]= async {
    if(_observer.isDefined)
      await { _observer.get.completed() }
  }

  def error(exception: Throwable): Future[Unit] = ???

  def next(value: Dom): Future[Unit] = async {

    println(value.success.print())

    if(_observer.isDefined)
      await { _observer.get.next(Dom("", null, null, value.success, DataNothing())) }
  }

  def subscribe(observer: Observer[Dom]): Unit = {
    _observer = Some(observer)
  }
}
