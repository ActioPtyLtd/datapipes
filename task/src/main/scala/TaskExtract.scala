import Common.Data.DataNothing
import Common._

import scala.concurrent.Future

class TaskExtract() extends Task {

  val dataSource = DataSourceFactory.create(DataNothing())

  def completed(): Future[Unit] = ???

  def error(exception: Throwable): Future[Unit] = ???

  def next(value: DataSet): Future[Unit] = dataSource.exec(DataNothing())

  override def subscribe(observer: Observer[Dom]): Unit = {
    val dsObserver = new Observer[DataSet] {
      def completed(): Future[Unit] = observer.completed()

      def error(exception: Throwable): Future[Unit] = observer.error(exception)

      def next(value: DataSet): Future[Unit] =
        observer.next(Dom("",null,null,value,DataNothing()))

    }
    dataSource.subscribe(dsObserver)
  }


}
