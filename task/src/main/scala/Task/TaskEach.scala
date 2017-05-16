package Task

import DataPipes.Common.Data._
import DataPipes.Common._

class TaskEach(val name: String, config: DataSet) extends Task {

  var _observer: Option[Observer[Dom]] = None

  def completed(): Unit = {
    _observer.get.completed()
  }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {

    val it = value.headOption.get.success.elems.toIterator // TODO fix

    while(it.hasNext)
    {
      _observer.get.next(value ~ Dom(name, null, List(), it.next(), DataNothing()))
    }
  }

  override def subscribe(observer: Observer[Dom]): Unit = {
    _observer = Some(observer)
  }
}
