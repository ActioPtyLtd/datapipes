package actio.datapipes.task

import actio.common.Data._
import actio.common.{DataSource, Dom, Observer, Task}
import actio.datapipes.dataSources.LocalFileSystemDataSource
import actio.datapipes.task.Term.TermExecutor

import scala.collection.mutable.ListBuffer

class TaskStage(val name: String, val config: DataSet) extends Task {

  private val dataSource: DataSource = DataSource(config("dataSource"))
  private val runDataSource: DataSource = DataSource(config("run_dataSource"))
  private val _observer: ListBuffer[Observer[Dom]] = ListBuffer()

  private val termCreate: TermLinkedTree = TaskLookup.getTermTree(config("dataSource")("query")("create"))
  private val termInitialise: TermLinkedTree = TaskLookup.getTermTree(config("dataSource")("query")("initialise"))
  private val termComplete: TermLinkedTree = TaskLookup.getTermTree(config("dataSource")("query")("complete"))

  private val namespace: String = config("namespace").stringOption.getOrElse("actio.datapipes.task.Term.Legacy.Functions")
  private val termExecutor = new TermExecutor(namespace)

  private var initialised = false

  def completed(): Unit = {
    _observer.foreach(o => o.completed())
  }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {

    // hack right now
    if(config("dataSource")("type").stringOption.contains("file")) {
      val t = new TaskFileDump(name, Operators.mergeLeft(config,DataRecord(config("dataSource")("directory"))))
      t.next(value)

      import JsonXmlDataSet._

      runDataSource.execute(config("run_dataSource"), DataRecord("create", DataString("line", value("start").success.toJson)))

    } else {
      if (!initialised) {
        val adjustedDom = Operators.mergeLeft(value,
          DataRecord(DataString("taskName", this.name)))
        val query = TaskLookup.interpolate(termExecutor, termInitialise, adjustedDom)
        dataSource.execute(config("dataSource"), query)
        initialised = true
      }

      val queries = value.headOption.toList.flatMap(d => d.success.map(s => TaskLookup.interpolate(termExecutor, termCreate, DataRecord(Operators.relabel(s, "data"), DataString("taskName", this.name)))))

      dataSource.execute(config("dataSource"), queries: _*)
    }
  }

  def subscribe(observer: Observer[Dom]): Unit = {
    _observer.append(observer)
  }

}
