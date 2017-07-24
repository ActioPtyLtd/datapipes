package actio.datapipes.pipescript.Pipeline

import actio.common.Data.{DataSet, Operators}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

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

    val pipes = ds(script)(pipelines).elems.toList.sortBy(s => s.label)// important to reverse so it can find pipe names
    val newpipes = topologySort(pipes)

    PipeScript(ds(script)(settings), tasks.values.toList, getPipes(tasks, newpipes), defaultPipeName)

  }

  def topologySort(pipes: List[DataSet]): List[DataSet] = {
    var dependencyMap: Map[String, Set[String]] = Map()
    var pipeMap: Map[String, DataSet] = Map()
    pipes.foreach(p => pipeMap += (p.label -> p))
    pipes.foreach(p => {
      val pname: String = p.label
      val p1: Array[String] = p("pipe")
        .stringOption
        .map(s => s.split("\\|")).get
      val p2 = p1.map(name => name.replace(" ", "")).filter(
        name => pipeMap.contains(name)
      )
      dependencyMap += (pname -> p2.toSet)
    })
    @tailrec
    def tsort(toPreds: Map[String, Set[String]], done: Iterable[String]): Iterable[String] = {
      val (noPreds, hasPreds) = toPreds.partition { _._2.isEmpty }
      if (noPreds.isEmpty) {
        if (hasPreds.isEmpty) done else sys.error(hasPreds.toString)
      } else {
        val found = noPreds.map { _._1 }
        tsort(hasPreds.mapValues { _ -- found }, done ++ found)
      }
    }
    val ret = tsort(dependencyMap, Seq())
    val buf = new ListBuffer[DataSet]
    ret.foreach(name => {
      buf += pipeMap.get(name).get
    })

    return buf.toList
  }

  @tailrec
  def getPipes(operations: Map[String, Operation], pipes: List[DataSet]): List[Operation] =
    pipes match {
      case Nil => operations.values.toList
      case (h::t) => {
        val p1 = h("pipe")
          .stringOption
          .map(s => s.split("\\|")
            .map(m =>
              operations(m.replace(" ", "")))
            .reduceLeft((a, b) => Pipe(h.label, a, b)))

        val p2 = p1
          .map(p => operations + (h.label -> p))

        val pipe = p2
          .getOrElse(operations)
//
//        val pipe = h("pipe")
//          .stringOption
//          .map(s => s.split("\\|")
//            .map(m =>
//              operations(m.replace(" ", "")))
//            .reduceLeft((a, b) => Pipe(h.label, a, b)))
//          .map(p => operations + (h.label -> p))
//          .getOrElse(operations)

        getPipes(pipe, t)
      }
    }

}
