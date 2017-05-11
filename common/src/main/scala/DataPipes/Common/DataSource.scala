package DataPipes.Common

import DataPipes.Common.Data.DataSet

import scala.concurrent.Future

trait DataSource extends Observable[DataSet] {

  def exec(parameters: Parameters): Future[Unit]
}
