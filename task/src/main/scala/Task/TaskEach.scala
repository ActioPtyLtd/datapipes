package Task

import DataPipes.Common.Data._
import DataPipes.Common._

import scala.async.Async.{async, await}
import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

class TaskEach(val name: String, config: DataSet) extends Task {

  var _observer: Option[Observer[Dom]] = None

  def completed(): Future[Unit] = async {
    await {
      _observer.get.completed()
    }
  }

  def error(exception: Throwable): Future[Unit] = ???

  def next(value: Dom): Future[Unit] = async {

    val it = value.headOption.get.success.elems.toIterator // TODO fix

    while(it.hasNext)
      await {
        _observer.get.next(value ~ Dom(name, null, List(), it.next(), DataNothing()))
      }
  }

  override def subscribe(observer: Observer[Dom]): Unit = {
    _observer = Some(observer)
  }
}
