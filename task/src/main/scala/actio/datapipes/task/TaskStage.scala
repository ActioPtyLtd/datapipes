package actio.datapipes.task

import java.util.UUID

import actio.common.Data._
import actio.common.{DataSource, Dom, Observer, Task}
import actio.datapipes.dataSources.LocalFileSystemDataSource
import actio.datapipes.task.Term.TermExecutor

import scala.collection.mutable.ListBuffer

class TaskStage(val name: String, val config: DataSet) extends Task {

  private val dataSource: DataSource = DataSource(config("dataSource"))
  private lazy val runDataSource: DataSource = DataSource(config("run_dataSource"))
  private val _observer: ListBuffer[Observer[Dom]] = ListBuffer()

  private val termCreate: TermLinkedTree = TaskLookup.getTermTree(config("dataSource")("query")("create"))
  private val termInitialise: TermLinkedTree = TaskLookup.getTermTree(config("dataSource")("query")("initialise"))
  private val termComplete: TermLinkedTree = TaskLookup.getTermTree(config("dataSource")("query")("complete"))

  private val namespace: String = config("namespace").stringOption.getOrElse("actio.datapipes.task.Term.Legacy.Functions")
  private val termExecutor = new TermExecutor(namespace)

  private var initialised = false
  private var batchNum = 1

  def completed(): Unit = {
    _observer.foreach(o => o.completed())
  }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {

    val dsInit = Operators.mergeLeft(
      Dom.dom2DataSet(value.headOption.get),
      DataRecord(
        value("start").success("run"),
        DataRecord("parameters", value("start").success.elems.collect { case s: DataString => s }.toList),
        DataString("batchId", batchNum.toString.reverse.padTo(4, '0').reverse)
      )
    )

    val ds = if(config("dataSource")("query")("create").stringOption.isDefined)
      TaskLookup.interpolate(termExecutor, termCreate, dsInit)
    else
      Operators.mergeLeft(TaskLookup.interpolate(termExecutor, termCreate, dsInit), dsInit)

    dataSource.execute(config("dataSource"), ds)

    batchNum = batchNum + 1

    //  val queries = value.headOption.toList.flatMap(d => d.success.map(s => TaskLookup.interpolate(termExecutor, termCreate, DataRecord(Operators.relabel(s, "data"), DataString("taskName", this.name)))))

    //  dataSource.execute(config("dataSource"), queries: _*)
    //}
  }

  def subscribe(observer: Observer[Dom]): Unit = {
    _observer.append(observer)
  }

}