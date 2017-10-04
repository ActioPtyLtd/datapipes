package actio.datapipes.dataSources

import actio.common.Data._
import actio.common.{DataSource, Observer}
import com.typesafe.scalalogging.Logger

import scala.collection.mutable.ListBuffer

class PagingDataSource(config: DataSet, dataSource: DataSource, evalQuery: DataSet => DataSet, stop: DataSet => Boolean) extends DataSource {
  private val logger = Logger("PagingDataSource")
  private val _observer: ListBuffer[Observer[DataSet]] = ListBuffer()
  var lastDataSet: DataSet = _

  lazy val dsObserver = new Observer[DataSet] {
    def completed(): Unit = {

    }

    def error(exception: Throwable): Unit = _observer.foreach(o => o.error(exception))

    def next(value: DataSet): Unit = {
      lastDataSet = value
      _observer.foreach(o => o.next(value))
    }

  }

  def executeRecursive(index: Int, query: DataSet): Unit = {

    val nquery = evalQuery(Operators.mergeLeft(query, DataRecord(DataNumeric("index", index))))

    lastDataSet = DataNothing()
    dataSource.execute(config, nquery)

    if (!stop(Operators.mergeLeft(lastDataSet,DataRecord(DataNumeric("index", index)))))
      executeRecursive(index + 1, query)
  }

  def execute(config: DataSet, query: DataSet*): Unit = {

    dataSource.subscribe(dsObserver)

    executeRecursive(0, config("query")("read"))

    _observer.foreach(o => o.completed())
  }

  def subscribe(observer: Observer[DataSet]): Unit = {
    _observer.append(observer)
  }
}