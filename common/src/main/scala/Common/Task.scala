package Common

import scala.concurrent.Future

trait Task {

  def execute(data: Data): DataSet

  //def executeAsync(data: Data): Future[DataSet]
}
