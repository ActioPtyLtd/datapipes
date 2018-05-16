package actio.datapipes.pipescript.Pipeline

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import actio.common.Data.{DataSet, DataString, Operators}
import actio.datapipes.pipescript.ConfigReader

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.meta.Term
import scala.util.Try

object PipeScriptBuilder {

  object Lang {
    val schedule = (c: DataSet) => c("schedule")
    object Schedule {
      val start = (c: DataSet) => schedule(c)("start_time")
      val end = (c: DataSet) => schedule(c)("end_time")
      val cron = (c: DataSet) => schedule(c)("cron")
      val interval = (c: DataSet) => schedule(c)("interval")
    }

  }


  def build(files: Seq[File]): (List[(File,PipeScript)], List[(File, Throwable)]) = {
    val result = files.map(f => (f, build(f))).toList
    val success = result.filter(f => f._2.isSuccess).map(m => (m._1, m._2.get))
    val failure = result.filter(f => f._2.isFailure).map(m => (m._1, m._2.failed.get))

    (success, failure)
  }

  def build(file: File): Try[PipeScript] = {
    Try(build(file.toString, ConfigReader.read(file)))
  }

  def build(file: File, mergeConfig: String): Try[(PipeScript,List[DataString])] = {
    val config = ConfigReader.read(file, mergeConfig)
    val parameters = config.elems.collect { case s: DataString => s }.toList
    Try((build(file.toString, config), parameters))
  }

  def build(name: String, ds: DataSet): PipeScript = {

    val startup = "startup"
    val exec = "exec"
    val script = "script"
    val taskList = "tasks"
    val taskType = "type"
    val pipe = "pipe"
    val pipelines = "pipelines"
    val settings = "settings"
    val service = "services"
    val bootstrap = "bootstrap"
    val schedule = "schedule"

    val defaultPipeName = ds(script)(startup)(exec).toString

    val tasks = ds(script)(taskList)
      .elems
      .map(t => t.label -> Task(t.label, t(taskType).toString, Operators.mergeLeft(t, ds(script)(settings))))
      .toMap

    val pipes = ds(script)(pipelines).elems.toList.sortBy(s => s.label) // important to reverse so it can find pipe names
    val newpipes = topologySort(pipes)

    val piplist = getPipes(tasks, newpipes)

    val services = ds(script)(service).elems.map(m => Service(
      m("path").toString,
      m("get").stringOption.map(g => piplist.find(f => f.name == g).get),
      m("put").stringOption.map(g => piplist.find(f => f.name == g).get),
      m("post").stringOption.map(g => piplist.find(f => f.name == g).get),
      m("patch").stringOption.map(g => piplist.find(f => f.name == g).get),
      m("proxyuri").stringOption.map((_, m("proxyport").intOption.getOrElse(80)))
    )).toList

    val scheduleSection = ds(script)(schedule)("directory").stringOption.map(m => {
      ConfigMonitorSchedule(m, ds(script)(schedule)("poll").intOption.getOrElse(10))
    })

    PipeScript(name, ds(script)(settings), services, tasks.values.toList, getPipeLines(piplist, newpipes), scheduleSection, defaultPipeName)

  }

  def topologySort(pipes: List[DataSet]): List[DataSet] = {
    var dependencyMap: Map[String, Set[String]] = Map()
    var pipeMap: Map[String, DataSet] = Map()
    pipes.foreach(p => pipeMap += (p.label -> p))
    pipes.foreach(p => {
      val pname: String = p.label
      val p1: Array[String] = p("pipe")
        .stringOption
        .map(s => s.split("&|\\||\\(|\\)")).get
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

  def getPipeOperation(term: Term, pipeName: String, operations: Map[String, Operation], prevOpName: String): (Operation, String) = term match {
    case Term.ApplyInfix(lTerm: Term, Term.Name("|"), Nil, Seq(rTerm: Term)) => {
      val left = getPipeOperation(lTerm, pipeName, operations, prevOpName)
      val right = getPipeOperation(rTerm, pipeName, operations, left._2)
      (Pipe(pipeName, left._1, right._1), right._2)
    }
    case Term.ApplyInfix(lTerm: Term, Term.Name("&"), Nil, Seq(rTerm: Term)) => {
      val left = getPipeOperation(lTerm, pipeName, operations, prevOpName)
      val right = getPipeOperation(rTerm, pipeName, operations, prevOpName)
      (SequenceOnFailOrSuccess(pipeName, left._1, right._1), right._2)
    }
    case Term.ApplyInfix(lTerm: Term, Term.Name("&&"), Nil, Seq(rTerm: Term)) => {
      val left = getPipeOperation(lTerm, pipeName, operations, prevOpName)
      val right = getPipeOperation(rTerm, pipeName, operations, prevOpName)
      (SequenceOnSuccess(pipeName, left._1, right._1), right._2)
    }
    case Term.Apply(Term.Name(name), args) => (Select(operations(name.replace(" ", "")), args.head.toString()), name)
    case Term.Name(name) => ({
      val task = operations(name.replace(" ", ""))
      task match {
        case Task(_, "stage_load", _) => task // if the task is a stage task, don't select, i.e. send root Dom
        case Task(_, "event", _) => task // if the task is a event task, don't select, i.e. send root Dom
        case Task(_, _, _) => Select(task, prevOpName) // in all other cases get specific Dom
        case _ => task
      }
    }, name)
  }

  def getPipeOperation(pipeName: String, str: String, operations: Map[String, Operation]): Operation = {
    import scala.meta._
    val p = str.parse[Term].get

    getPipeOperation(p, pipeName, operations, "start")._1
  }

  @tailrec
  def getPipes(operations: Map[String, Operation], pipes: List[DataSet]): List[Operation] =
    pipes match {
      case Nil => operations.values.toList
      case (h :: t) => {
        val p1 = h("pipe")
          .stringOption
          .map(s => getPipeOperation(h.label, s, operations))

        val p2 = p1
          .map(p => operations + (h.label -> p))

        val pipe = p2
          .getOrElse(operations)

        getPipes(pipe, t)
      }
    }

  def getPipeLines(operations: List[Operation], pipes: List[DataSet]): List[Pipeline] = {
    operations.flatMap(o => pipes.find(f => f.label == o.name).map(p => (o, p))).map(m => Pipeline(m._1.name, m._1, getScheduleFromPipe(m._2)))
  }

  val yyyyMMdd = "yyyy-MM-dd HH:mm:ss"
  val d1900 = "1900-01-01 00:00:00"
  val d2999 = "2999-01-01 00:00:00"

  def getScheduleFromPipe(config: DataSet): Schedule =
    if(Lang.Schedule.cron(config).stringOption.isDefined)
      RunWithCronSchedule(
        new SimpleDateFormat(yyyyMMdd).parse(Lang.Schedule.start(config).stringOption.getOrElse(d1900)),
        new SimpleDateFormat(yyyyMMdd).parse(Lang.Schedule.end(config).stringOption.getOrElse(d2999)),
        Lang.Schedule.cron(config).toString)
    else if(Lang.Schedule.interval(config).intOption.isDefined)
      RunWithInterval(new SimpleDateFormat(yyyyMMdd).parse(Lang.Schedule.start(config).stringOption.getOrElse(d1900)),
        new SimpleDateFormat(yyyyMMdd).parse(Lang.Schedule.end(config).stringOption.getOrElse(d2999)),
        Lang.Schedule.interval(config).intOption.get)
    else if(Lang.schedule(config).isDefined)
      RunOnce(new SimpleDateFormat(yyyyMMdd).parse(Lang.Schedule.end(config).stringOption.getOrElse(d2999)))
    else
      RunNever()
}
