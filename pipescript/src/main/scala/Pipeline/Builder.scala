package Pipeline

import DataPipes.Common.Data._

object Builder {

  def build(ds: DataSet): Option[PipeScript] = {

    val startup = "startup"
    val exec = "exec"
    val script = "script"
    val taskList = "tasks"
    val taskType = "type"
    val pipe = "pipe"
    val pipelines = "pipelines"
    val settings = "settings"

    val pipeName = ds(script)(startup)(exec).stringOption.getOrElse("")

    val tasks = ds(script)(taskList)
      .elems
      .map(t => t.label -> Task(t.label, t(taskType).stringOption.getOrElse(""), Operators.mergeLeft(t, ds(script)(settings))))
      .toMap

    val pipeExec = ds(script)(pipelines)(pipeName)(pipe)
      .stringOption
      .map(s => s.split("\\|")
        .map(m => tasks(m.replace(" ", "")).asInstanceOf[Operation])
        .reduceLeft((a,b) => Pipe(pipeName, a, b)))

    pipeExec.map(p => PipeScript(ds(script)(settings), tasks.values.toList, p))

  }
}
