package Task

import Common.{DataSet, Dom, Observer}
import Common.Data.PrettyPrint.PrettyPrint

import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class TaskPrint(val name: String, config: DataSet) extends Common.Task {
  var _observer: Option[Observer[Dom]] = None

  val formatLookup: Map[String,(DataSet => String)] = Map(
    "" -> (ds => ds.print),
    "xml" -> (ds => ds.toXml),
    "json" -> (ds => ds.toJson))

  val format = config("format").stringOption.getOrElse("")

  def completed(): Future[Unit]= async {
    if(_observer.isDefined)
      await { _observer.get.completed() }
  }

  def error(exception: Throwable): Future[Unit] = ???

  def next(value: Dom): Future[Unit] = async {

    println(value.headOption.map(s => formatLookup(format)(s.success)).getOrElse(""))

    if(_observer.isDefined)
      await { _observer.get.next(value) }
  }

  def subscribe(observer: Observer[Dom]): Unit = {
    _observer = Some(observer)
  }
}
