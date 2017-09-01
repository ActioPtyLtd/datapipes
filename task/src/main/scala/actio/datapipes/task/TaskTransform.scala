package actio.datapipes.task

import actio.common.Data.{DataNothing, DataSet}
import actio.common.{Dom, Observer, Task}

import scala.collection.mutable.ListBuffer

abstract class TaskTransform(val name: String) extends Task {

  val _observer: ListBuffer[Observer[Dom]] = ListBuffer()

  def completed(): Unit = _observer.foreach(o => o.completed())

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {

    val nds = transform(value)

    val send = for {
      ds <- nds
      o <- _observer
    } yield (o, ds)

    send.foreach(s => s._1.next(Dom(name, List(), s._2, DataNothing(), Nil)))

  }

  def subscribe(observer: Observer[Dom]): Unit = _observer.append(observer)

  def transform(dom: Dom): Seq[DataSet]

}