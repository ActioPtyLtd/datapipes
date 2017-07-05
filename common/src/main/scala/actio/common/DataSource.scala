package actio.common

import actio.common.Data.DataSet

trait DataSource extends Observable[DataSet] {

  def execute(config: DataSet, query: DataSet*): Unit
}
