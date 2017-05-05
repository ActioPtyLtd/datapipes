package Task

import Common.Data.{DataArray, DataNothing}
import Common._

import scala.async.Async.{async, await}
import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class TaskBatch(val name: String, val size: Int) extends Task {

  var _observer: Option[Observer[Dom]] = None
  var buffer: Queue[DataSet] = Queue()

  def completed(): Future[Unit] = async {
    if(buffer.nonEmpty && _observer.isDefined) {
      await {
        _observer.get.next(Dom("", null, null, DataArray(buffer.toList), DataNothing()))
      }
      await {
        _observer.get.completed()
      }
    }

  }

  def error(exception: Throwable): Future[Unit] = ???

  def next(value: Dom): Future[Unit] = async {

    buffer = buffer.enqueue(value.success)

    if(buffer.size == size && _observer.isDefined) {
      await {
        _observer.get.next(Dom("", null, null, DataArray(buffer.toList), DataNothing()))
      }
      buffer = Queue()
    }
  }

  override def subscribe(observer: Observer[Dom]): Unit = {
    _observer = Some(observer)
  }
}
