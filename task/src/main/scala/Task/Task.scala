package Task

import Common.{DataSet, Task}

import scala.meta._

object Task {

  private lazy val tasks =  Map(
    "extract" -> ((name: String, config: DataSet) =>
      new TaskExtract(name, config)),
    "term" -> ((name: String, config: DataSet) =>
      new TaskTerm(name, config("term").stringOption.getOrElse("").parse[Term].get)),
    "batch" -> ((name: String, config: DataSet) =>
      new TaskBatch(name, config)),
    "print" -> ((name: String, config: DataSet) =>
      new TaskPrint(name)))

  def apply(name: String, taskType: String, config: DataSet): Task = tasks(taskType)(name, config)
}
