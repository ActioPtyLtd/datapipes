package Pipeline

import DataPipes.Common.Data._

case class Task(name: String, taskType: String, config: DataSet) extends Operation {

}
