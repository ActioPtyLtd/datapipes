package Task

import Common.{DataSet, Task}

import scala.meta._

object Task {

  private lazy val tasks =  Map(
    "TaskExtract" -> ((name: String, config: DataSet) => new TaskExtract(name, config)),
    "TaskTerm" -> ((name: String, config: DataSet) => new TaskTerm(name, config("term").stringOption.getOrElse("").parse[Term].get)),
    "TaskBatch" -> ((name: String, config: DataSet) => new TaskBatch(name, config("size").stringOption.map(m => m.toInt).getOrElse(100))),
    "TaskPrint" -> ((name: String, config: DataSet) => new TaskPrint(name)))

  def apply(name: String, taskType: String, config: DataSet): Task = tasks(taskType)(name, config)
}
