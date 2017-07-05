package actio.datapipes.pipescript.Pipeline

import actio.common.Data.DataSet

case class Task(name: String, taskType: String, config: DataSet) extends Operation {

}
