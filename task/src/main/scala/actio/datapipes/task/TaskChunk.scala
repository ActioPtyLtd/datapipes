package actio.datapipes.task

import actio.common.Data.{DataArray, DataNothing, DataSet}
import actio.common.{DataSource, Dom, Observer, Task}
import actio.datapipes.task.Term.TermExecutor

import scala.collection.mutable.ListBuffer

class TaskChunk(val name: String, val config: DataSet) extends Task {
  private val _observer: ListBuffer[Observer[Dom]] = ListBuffer()

  def completed(): Unit = {
    _observer.foreach(o => o.completed())
  }

  def error(exception: Throwable): Unit = _observer.foreach(o => o.error(exception))

  def next(value: Dom): Unit = {

    val groups = value.success.elems.toList.grouped(config("size").intOption.getOrElse(100))

    _observer.foreach { o =>
      groups.foreach { g =>
        o.next(Dom(name, Nil, DataArray(g), DataNothing(), Nil))
      }
    }
  }

  def subscribe(observer: Observer[Dom]): Unit = {
    _observer.append(observer)
  }

}
