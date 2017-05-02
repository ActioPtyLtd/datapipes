package Common

import scala.concurrent.Future

trait DataSource extends Observable[DataSet] {

  def exec(parameters: Parameters): Future[Unit]
}
