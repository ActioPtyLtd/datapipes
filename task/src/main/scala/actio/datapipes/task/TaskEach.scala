package actio.datapipes.task

import actio.common.Data.{DataArray, DataNothing, DataSet}
import actio.common.{Dom, Observer, Task}

import scala.collection.mutable.ListBuffer

class TaskEach(val name: String, config: DataSet) extends Task {

  val _observer: ListBuffer[Observer[Dom]] = ListBuffer()

  def completed(): Unit = {
    _observer.foreach(o => o.completed())
  }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {

    val it = value.headOption.get.success.elems.toIterator // TODO fix

    val element = DataArray(value.headOption.toList.flatMap(m => m.success.flatMap(s => s.elems)))
Dom()
    _observer.foreach(o => o.next(value ~ Dom(name, List(), element, DataNothing(), Nil)))

  }

  override def subscribe(observer: Observer[Dom]): Unit = {
    _observer.append(observer)
  }
}
