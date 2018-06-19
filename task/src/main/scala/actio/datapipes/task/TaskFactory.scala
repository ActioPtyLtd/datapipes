package actio.datapipes.task

import actio.common.Data.DataSet
import actio.datapipes.task.Legacy.TaskFunctionFold


object TaskFactory {

  private lazy val tasks = Map[String, (String, DataSet, TaskSetting) => actio.common.Task](
    "extract" -> ((name, config, setting) =>
      new TaskExtract(name, config, setting)),
    "load" -> ((name, config, setting) =>
      new TaskLoad(name, config, setting)),
    "transformTerm" -> ((name, config, setting) =>
      new TaskTerm(name, config, setting)),
    "assert" -> ((name, config, setting) =>
      new TaskAssert(name, config, setting)),
    "transform" -> ((name, config, _) =>
      new TaskFunctionFold(name, config)),
    "chunk" -> ((name, config, _) =>
      new TaskChunk(name, config)),
    "mergeTemplate" -> ((name, config, setting) =>
      new TaskTemplate(name, config, setting)),
    "each" -> ((name, config, _) =>
      new TaskEach(name, config)),
    "lookup" -> ((name, config, setting) =>
      new TaskLookup(name, config, setting)),
    "datasourceupdate" -> ((name, config, setting) =>
      new TaskUpdate(name, config, setting)),
    "join" -> ((name, config, setting) =>
      new TaskJoin(name, config, setting)),
    "fileDump" -> ((name, config, setting) =>
      new TaskFileDump(name, config)),
    "stage_load" -> ((name, config, setting) =>
      new TaskStage(name, config, setting)),
    "merge_load" -> ((name, config, setting) =>
      new TaskMergeLoad(name, config, setting)),
    "dump" -> ((name, config, _) =>
      new TaskPrint(name, config)))

  def apply(name: String, taskType: String, config: DataSet): actio.common.Task =
    tasks(taskType)(name, config, TaskSetting(config("version").stringOption.getOrElse("v2"),config("namespace").stringOption.getOrElse("actio.datapipes.task.Term.Functions"),config("traversal").stringOption.contains("strict")) )
}
