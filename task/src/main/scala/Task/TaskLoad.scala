package Task

import DataPipes.Common.Data._
import DataPipes.Common._
import Term.TermExecutor

import scala.collection.mutable.{ ListBuffer, Queue }
import scala.util.Try

class TaskLoad(val name: String, val config: DataSet) extends Task {

  private val dataSource: DataSource = DataSource(config("dataSource"))
  private val _observer: ListBuffer[Observer[Dom]] = ListBuffer()
  private val terms: TermLinkedTree = TaskLookup.getTermTree(config("dataSource")("query")("create"))
  private val namespace: String = config("namespace").stringOption.getOrElse("Term.Legacy.Functions")
  private val termExecutor = new TermExecutor(namespace)

  def completed(): Unit = { Unit }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {
    val creates = value.headOption.map(h => h.success.map(i => if (config("dataSource")("query")("create").toOption.isDefined)
      TaskLookup.interpolate(termExecutor, terms, i)
    else i))

    creates.foreach(c =>
      if(c.nonEmpty)
        dataSource.execute(config("dataSource"), c: _*))
  }

  def subscribe(observer: Observer[Dom]): Unit = {
    _observer.append(observer)
  }

}
