import Common.Data.DataNothing
import Common._

import scala.concurrent.Future

class TaskExtract() extends Task {

  val dataSource = DataSourceFactory.create(DataNothing())

  override def completed(): Future[Unit] = ???

  override def error(exception: Throwable): Future[Unit] = ???

  override def next(value: DataSet): Future[Unit] = dataSource.exec(DataNothing())

  override def subscribe(observer: Observer[Dom]): Unit = {
    val dsObserver = new Observer[DataSet] {
      override def completed(): Future[Unit] = observer.completed()

      override def error(exception: Throwable): Future[Unit] = observer.error(exception)

      override def next(value: DataSet): Future[Unit] =
        observer.next(Dom("",null,null,value,DataNothing()))

    }
    dataSource.subscribe(dsObserver)
  }


}
