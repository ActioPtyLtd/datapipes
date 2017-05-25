package DataPipes.Common

import DataPipes.Common.Data.DataSet

import scala.concurrent.Future

trait DataSource extends Observable[DataSet] {

  def execute(config: DataSet, query: DataSet*): Unit
}
