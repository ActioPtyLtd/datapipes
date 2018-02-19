package actio.datapipes.task

import actio.common.Data.DataSet
import actio.datapipes.task.Legacy.TaskFunctionFold


object TaskFactory {

  private lazy val tasks = Map[String, (String, DataSet, String) => actio.common.Task](
    "extract" -> ((name, config, version) =>
      new TaskExtract(name, config, version)),
    "load" -> ((name, config, version) =>
      new TaskLoad(name, config, version)),
    "transformTerm" -> ((name, config, version) =>
      new TaskTerm(name, config, version)),
    "assert" -> ((name, config, _) =>
      new TaskAssert(name, config)),
    "transform" -> ((name, config, _) =>
      new TaskFunctionFold(name, config)),
    "mergeTemplate" -> ((name, config, version) =>
      new TaskTemplate(name, config, version)),
    "each" -> ((name, config, _) =>
      new TaskEach(name, config)),
    "lookup" -> ((name, config, version) =>
      new TaskLookup(name, config, version)),
    "datasourceupdate" -> ((name, config, version) =>
      new TaskUpdate(name, config, version)),
    "join" -> ((name, config, version) =>
      new TaskJoin(name, config, version)),
    "fileDump" -> ((name, config, version) =>
      new TaskFileDump(name, config)),
    "stage_load" -> ((name, config, _) =>
      new TaskStage(name, config)),
    "merge_load" -> ((name, config, _) =>
      new TaskMergeLoad(name, config)),
    "dump" -> ((name, config, _) =>
      new TaskPrint(name, config)))

  def apply(name: String, taskType: String, config: DataSet): actio.common.Task =
    tasks(taskType)(name, config, config("version").stringOption.getOrElse("v1") )
}
