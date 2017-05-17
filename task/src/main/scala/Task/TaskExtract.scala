package Task

import DataPipes.Common._
import DataPipes.Common.Data._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class TaskExtract(val name: String, val config: DataSet) extends Task {

  val dataSource: DataSource = DataSource(config("dataSource"))

  def completed(): Unit = { Unit }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = dataSource.execute(config("dataSource"), DataNothing())

  def subscribe(observer: Observer[Dom]): Unit = {

    val dsObserver = new Observer[DataSet] {
      def completed(): Unit = observer.completed()

      def error(exception: Throwable): Unit = observer.error(exception)

      def next(value: DataSet): Unit =
        observer.next(Dom() ~ Dom(name,null,Nil,value,DataNothing()))
    }

    dataSource.subscribe(dsObserver)
  }


}
