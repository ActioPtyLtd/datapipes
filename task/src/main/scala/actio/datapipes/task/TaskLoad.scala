package actio.datapipes.task

import Term.TermExecutor
import actio.common.Data.DataSet
import actio.common.{DataSource, Dom, Observer, Task}

import scala.collection.mutable.{ListBuffer}

class TaskLoad(val name: String, val config: DataSet, taskSetting: TaskSetting) extends Task {

  private val dataSource: DataSource = DataSourceFactory(config("dataSource"), taskSetting)
  private val _observer: ListBuffer[Observer[Dom]] = ListBuffer()
  private val terms: TermLinkedTree = TaskLookup.getTermTree(TaskLookup.queryAdjust(config("dataSource")("query")("create"), taskSetting.version))
  private val namespace: String = config("namespace").stringOption.getOrElse("actio.datapipes.task.Term.Legacy.Functions")
  private val termExecutor = new TermExecutor(taskSetting)

  def completed(): Unit = {
    _observer.foreach(o => o.completed())
  }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {
    val creates = value.success.map(i => if (config("dataSource")("query")("create").toOption.isDefined)
      TaskLookup.interpolate(termExecutor, terms, i)
    else i)

      if(creates.nonEmpty)
        dataSource.execute(config("dataSource"), creates: _*)

    _observer.foreach(o => o.next(value))
  }

  def subscribe(observer: Observer[Dom]): Unit = {
    _observer.append(observer)
  }

}
