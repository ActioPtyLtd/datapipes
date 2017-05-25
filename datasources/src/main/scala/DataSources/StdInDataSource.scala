package DataSources

import DataPipes.Common.Data._
import DataPipes.Common._

class StdInDataSource extends DataSource {

  var _observer: Option[Observer[DataSet]] = None

  def execute(config: DataSet, query: DataSet): Unit = {
    var line: String = ""

    do {
      line = scala.io.StdIn.readLine()
      if (line != null)
      {
        _observer.get.next(DataString(line))
      }
    } while (line != null)


    _observer.get.completed()

  }

  def subscribe(observer: Observer[DataSet]): Unit = _observer = Some(observer)

  def execute(config: DataSet, query: DataSet*): Unit = {
    query.foreach(q => execute(config, q))
  }
}
