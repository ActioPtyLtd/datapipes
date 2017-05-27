package Task

import DataPipes.Common.Data._
import DataPipes.Common._

import scala.collection.mutable.ListBuffer

class TaskEach(val name: String, config: DataSet) extends Task {

  val _observer: ListBuffer[Observer[Dom]] = ListBuffer()

  def completed(): Unit = {
    _observer.foreach(o => o.completed())
  }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {

    val it = value.headOption.get.success.elems.toIterator // TODO fix

    val send = for {
      dom <- value.headOption.toList
      element <- dom.success.elems
      observer <- _observer
    } yield (observer, element)

    send.foreach(s => s._1.next(value ~ Dom(name, List(), s._2, DataNothing())))
  }

  override def subscribe(observer: Observer[Dom]): Unit = {
    _observer.append(observer)
  }
}
