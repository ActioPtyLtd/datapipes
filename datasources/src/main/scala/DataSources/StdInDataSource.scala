package DataSources

import DataPipes.Common.Data._
import DataPipes.Common._
import scala.concurrent.Future
import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global

class StdInDataSource extends DataSource {

  var _observer: Option[Observer[DataSet]] = None

  def exec(parameters: Parameters): Future[Unit] = async {
    var line: String = ""

    do {
      line = scala.io.StdIn.readLine()
      if(line != null)
        await { _observer.get.next(DataString(line)) }
    } while(line != null)

    await { _observer.get.completed() }
  }

  def subscribe(observer: Observer[DataSet]): Unit = _observer = Some(observer)
}
