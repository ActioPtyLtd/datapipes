package actio.datapipes.dataSources

import DataPipes.Common.Data._
import DataPipes.Common._

import scala.collection.mutable.ListBuffer

class StdInDataSource extends DataSource {

  private val _observer: ListBuffer[Observer[DataSet]] = ListBuffer()

  def execute(config: DataSet, query: DataSet): Unit = {
    var line: String = ""

    do {
      line = scala.io.StdIn.readLine()
      if (line != null) {
        _observer.foreach(o => o.next(DataString(line)))
      }
    } while (line != null)

    _observer.foreach(o => o.completed())
  }

  def subscribe(observer: Observer[DataSet]): Unit = _observer.append(observer)

  def execute(config: DataSet, query: DataSet*): Unit = {
    if(query.nonEmpty)
      query.foreach(q => execute(config, q))
    else
      execute(config, DataNothing())
  }
}
