package Task

import Common.{DataSet, Task}

import scala.meta._

object Task {
  def apply(name: String, taskType: String, config: DataSet): Task =
    if(taskType == "TaskExtract")
      new TaskExtract(name, config)
    else if(taskType == "TaskTerm")
      new TaskTerm(name, config("term").stringOption.getOrElse("").parse[Term].get)
    else
      new TaskPrint(name)
}
