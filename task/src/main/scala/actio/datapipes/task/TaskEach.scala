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

    val element = DataArray(value.success.flatMap(s => s.elems).toList)
Dom()
    _observer.foreach(o => o.next(Dom(name, List(), element, DataNothing(), Nil)))

  }

  override def subscribe(observer: Observer[Dom]): Unit = {
    _observer.append(observer)
  }
}
