package Task

import DataPipes.Common.Data.DataSet
import Legacy._

object Task {

  private lazy val tasks = Map[String, (String, DataSet, String) => DataPipes.Common.Task](
    "extract" -> ((name, config, _) =>
      new TaskExtract(name, config)),
    "transformTerm" -> ((name, config, version) =>
      new TaskTerm(name, config, version)),
    "transform" -> ((name, config, _) =>
      new TaskFunctionFold(name, config)),
    "mergeTemplate" -> ((name, config, version) =>
      new TaskTemplate(name, config, version)),
    "batch" -> ((name, config, _) =>
      new TaskBatch(name, config)),
    "each" -> ((name, config, _) =>
      new TaskEach(name, config)),
    "lookup" -> ((name, config, _) =>
      new TaskLookup(name, config)),
    "datasourceupdate" -> ((name, config, version) =>
      new TaskUpdate(name, config, version)),
    "print" -> ((name, config, _) =>
      new TaskPrint(name, config)))

  def apply(name: String, taskType: String, config: DataSet): DataPipes.Common.Task =
    tasks(taskType)(name, config, config("version").stringOption.getOrElse("v1") )
}
