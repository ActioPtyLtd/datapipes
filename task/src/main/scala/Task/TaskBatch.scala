package Task

import DataPipes.Common._
import DataPipes.Common.Data._

import scala.collection.immutable.Queue
import scala.util.Try

class TaskBatch(val name: String, config: DataSet) extends Task {

  val size: Int = config("size").stringOption.flatMap(m => Try(m.toInt).toOption).getOrElse(100)
  var _observer: Option[Observer[Dom]] = None
  var buffer: Queue[DataSet] = Queue()

  def completed(): Unit = {
    if(buffer.nonEmpty && _observer.isDefined) {
      _observer.get.next(Dom("", List(), DataArray(buffer.toList), DataNothing()))
      _observer.get.completed()
    }

  }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {

    buffer = buffer.enqueue(value.headOption.get.success) //TODO fix this

    if(buffer.size == size && _observer.isDefined)
    {
      _observer.get.next(value ~ Dom(name, List(), DataArray(buffer.toList), DataNothing()))
      buffer = Queue()
    }
  }

  override def subscribe(observer: Observer[Dom]): Unit = {
    _observer = Some(observer)
  }
}
