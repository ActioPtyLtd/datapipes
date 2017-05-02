package Task

import Common.{Dom, Observer}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class TaskPrint(val name: String) extends Common.Task {

  def completed(): Future[Unit]= Future { println("done") }

  def error(exception: Throwable): Future[Unit] = ???

  def next(value: Dom): Future[Unit] = Future { println(value.success) }

  def subscribe(observer: Observer[Dom]): Unit = {}
}
