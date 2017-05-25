package Task

import DataPipes.Common.Data._
import DataPipes.Common._
import Term.TermExecutor

import scala.collection.mutable.{ListBuffer, Queue}
import scala.util.Try

class TaskLoad(val name: String, val config: DataSet) extends Task {

  val dataSource: DataSource = DataSource(config("dataSource"))
  private val _observer: ListBuffer[Observer[Dom]] = ListBuffer()
  val terms = TaskLookup.getTermTree(config("dataSource")("query")("create"))
  val namespace = config("namespace").stringOption.getOrElse("Term.Functions")
  val termExecutor = new TermExecutor(namespace)

  def completed(): Unit = { Unit }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {
    val creates = value.headOption.map(h => h.success.map(i => TaskLookup.interpolate(termExecutor, terms, i)))

  creates.foreach(c =>
    dataSource.execute(config("dataSource"), c: _*))
  }

  def subscribe(observer: Observer[Dom]): Unit = {
    _observer.append(observer)
  }


}
