package DataSources

import Common.Data.DataString
import Common.{DataSet, DataSource, Observer, Parameters}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class StdInDataSource extends DataSource {

  var _observer: Option[Observer[DataSet]] = None

  def exec(parameters: Parameters): Future[Unit] = Future {
    var line: String = ""
    do {
      line = scala.io.StdIn.readLine()
      _observer.get.next(DataString("", line))
    } while(line != null)
    _observer.get.completed()
  }

  def subscribe(observer: Observer[DataSet]): Unit = _observer = Some(observer)
}
