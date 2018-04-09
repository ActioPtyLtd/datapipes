package actio.datapipes.task

import java.util.UUID

import actio.common.Data._
import actio.common.{DataSource, Dom, Observer, Task}
import actio.datapipes.dataSources.LocalFileSystemDataSource
import actio.datapipes.task.Term.TermExecutor

import scala.collection.mutable.ListBuffer

class TaskStage(val name: String, val config: DataSet) extends Task {

  private val dataSource: DataSource = DataSourceFactory(config("dataSource"))
  private lazy val runDataSource: DataSource = DataSourceFactory(config("run_dataSource"))
  private val _observer: ListBuffer[Observer[Dom]] = ListBuffer()

  private val termCreate: TermLinkedTree = TaskLookup.getTermTree(config("dataSource")("query")("create"))
  private val termInitialise: TermLinkedTree = TaskLookup.getTermTree(config("dataSource")("query")("initialise"))
  private val termComplete: TermLinkedTree = TaskLookup.getTermTree(config("dataSource")("query")("finalise"))

  private val namespace: String = config("namespace").stringOption.getOrElse("actio.datapipes.task.Term.Legacy.Functions")
  private val termExecutor = new TermExecutor(namespace)

  private var initialised = false
  private var batchNum = 1

  def completed(): Unit = {
    if(config("dataSource")("query")("finalise").isDefined)
      dataSource.execute(config("dataSource"), config("dataSource")("query")("finalise"))
    
    _observer.foreach(o => o.completed())
  }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {

    val dsInit = Operators.mergeLeft(
      Dom.dom2DataSet(value.headOption.get),
      DataRecord(
        value("start").success(0)("run"),
        Operators.mergeLeft(config("parameters").toOption.getOrElse(DataRecord("parameters")),
        DataRecord("parameters", value("start").success(0).elems.collect { case s: DataString => s }.toList)),
        DataString("batchId", batchNum.toString.reverse.padTo(4, '0').reverse)
      )
    )

    if(!initialised && config("dataSource")("query")("initialise").stringOption.isDefined) {
      dataSource.execute(config("dataSource"),TaskLookup.interpolate(termExecutor, termInitialise, dsInit))
      initialised = true
    }

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
