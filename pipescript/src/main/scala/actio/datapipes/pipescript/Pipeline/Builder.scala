package actio.datapipes.pipescript.Pipeline

import DataPipes.Common.Data._

import scala.annotation.tailrec

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

    val pipes = ds(script)(pipelines).elems.toList.reverse // important to reverse so it can find pipe names

    PipeScript(ds(script)(settings), tasks.values.toList, getPipes(tasks, pipes), defaultPipeName)

  }

  @tailrec
  def getPipes(operations: Map[String, Operation], pipes: List[DataSet]): List[Operation] =
    pipes match {
      case Nil => operations.values.toList
      case (h::t) => {

        val pipe = h("pipe")
          .stringOption
          .map(s => s.split("\\|")
            .map(m =>
              operations(m.replace(" ", "")))
            .reduceLeft((a, b) => Pipe(h.label, a, b)))
          .map(p => operations + (h.label -> p))
          .getOrElse(operations)

        getPipes(pipe, t)
      }
    }

}
