package Pipeline

import Common.DataSet

case class Task(name: String, taskType: String, config: DataSet) extends Operation {

}
