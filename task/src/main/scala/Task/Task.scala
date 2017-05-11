package Task

import DataPipes.Common.Data.DataSet

object Task {

  private lazy val tasks =  Map[String,(String,DataSet) => DataPipes.Common.Task](
    "extract" -> ((name, config) =>
      new TaskExtract(name, config)),
    "term" -> ((name, config) =>
      new TaskTerm(name, config)),
    "template" -> ((name,config) =>
      new TaskTemplate(name, config)),
    "batch" -> ((name, config) =>
      new TaskBatch(name, config)),
    "print" -> ((name,config) =>
      new TaskPrint(name, config)))

  def apply(name: String, taskType: String, config: DataSet): DataPipes.Common.Task = tasks(taskType)(name, config)
}
