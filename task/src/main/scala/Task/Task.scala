package Task

import Common.{DataSet, Task}



object Task {

  private lazy val tasks =  Map[String,(String,DataSet) => Task](
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

  def apply(name: String, taskType: String, config: DataSet): Task = tasks(taskType)(name, config)
}
