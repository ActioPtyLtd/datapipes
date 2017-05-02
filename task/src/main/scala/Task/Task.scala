package Task

import Common.{DataSet, Task}

object Task {
  def apply(name: String, taskType: String, config: DataSet): Task =
    if(taskType == "TaskExtract")
      new TaskExtract(name, config)
    else
      new TaskPrint(name)
}
