package Task

import Common.Data.DataNothing
import Common._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class TaskExtract(val name: String, val config: DataSet) extends Task {

  val dataSource: DataSource = DataSource(config("dataSource"))

  def completed(): Future[Unit] = Future { Unit }

  def error(exception: Throwable): Future[Unit] = ???

  def next(value: Dom): Future[Unit] = dataSource.exec(config("dataSource"))

  def subscribe(observer: Observer[Dom]): Unit = {

    val dsObserver = new Observer[DataSet] {
      def completed(): Future[Unit] = observer.completed()

      def error(exception: Throwable): Future[Unit] = observer.error(exception)

      def next(value: DataSet): Future[Unit] =
        observer.next(Dom() ~ Dom(name,null,Nil,value,DataNothing()))
    }

    dataSource.subscribe(dsObserver)
  }


}
