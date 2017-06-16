package Pipeline

import DataPipes.Common.Data._

object Builder {

  def build(ds: DataSet): PipeScript = {

    val startup = "startup"
    val exec = "exec"
    val script = "script"
    val taskList = "tasks"
    val taskType = "type"
    val pipe = "pipe"
    val pipelines = "pipelines"
    val settings = "settings"

    val defaultPipeName = ds(script)(startup)(exec).stringOption.getOrElse("")

    val tasks = ds(script)(taskList)
      .elems
      .map(t => t.label -> Task(t.label, t(taskType).stringOption.getOrElse(""), Operators.mergeLeft(t, ds(script)(settings))))
      .toMap

    val pipeExec = ds(script)(pipelines).map(p => p(pipe)
      .stringOption
      .map(s => s.split("\\|")
        .map(m => tasks(m.replace(" ", "")).asInstanceOf[Operation])
        .reduceLeft((a,b) => Pipe(p.label, a, b))))
      .toList

    PipeScript(ds(script)(settings), tasks.values.toList, pipeExec.flatMap(f => f), defaultPipeName)

  }
}
