package Task

import DataPipes.Common.Data.DataSet
import Legacy._

object Task {

  private lazy val tasks = Map[String, (String, DataSet) => DataPipes.Common.Task](
    "extract" -> ((name, config) =>
      new TaskExtract(name, config)),
    "transformTerm" -> ((name, config) =>
      new TaskTerm(name, config)),
    "transform" -> ((name, config) =>
      new TaskFunctionFold(name, config)),
    "mergeTemplate" -> ((name, config) =>
      new TaskTemplate(name, config)),
    "batch" -> ((name, config) =>
      new TaskBatch(name, config)),
    "each" -> ((name, config) =>
      new TaskEach(name, config)),
    "lookup" -> ((name, config) =>
      new TaskLookup(name, config)),
    "datasourceupdate" -> ((name, config) =>
      new TaskUpdate(name, config)),
    "print" -> ((name, config) =>
      new TaskPrint(name, config)))

  def apply(name: String, taskType: String, config: DataSet): DataPipes.Common.Task = tasks(taskType)(name, config)
}
