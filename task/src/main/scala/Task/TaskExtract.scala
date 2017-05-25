package Task

import DataPipes.Common._
import DataPipes.Common.Data._
import Term.TermExecutor

import scala.util.Try
import scala.collection.mutable.{ListBuffer, Queue}

class TaskExtract(val name: String, val config: DataSet) extends Task {

  val size: Int = config("size").stringOption.flatMap(m => Try(m.toInt).toOption).getOrElse(100)
  val dataSource: DataSource = DataSource(config("dataSource"))
  private val _observer: ListBuffer[Observer[Dom]] = ListBuffer()
  val buffer = Queue[DataSet]()
  private val namespace = config("namespace").stringOption.getOrElse("Term.Functions")
  private val termExecutor = new TermExecutor(namespace)
  private val termRead = TaskLookup.getTermTree(config("dataSource")("query")("read"))

  def completed(): Unit = { Unit }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {
    dataSource.subscribe(dsObserver)

    val query = TaskLookup.interpolate(termExecutor, termRead,
      value.headOption.map(m => m.success).getOrElse(DataNothing()))


    dataSource.execute(config("dataSource"), query)
  }

  lazy val dsObserver = new Observer[DataSet] {
    def completed(): Unit = {
      if(buffer.nonEmpty) {
        _observer.foreach(o => o.next(Dom() ~ Dom(name, null, List(), DataArray(buffer.toList), DataNothing())))
        _observer.foreach(o => o.completed())
      }
    }

    def error(exception: Throwable): Unit = _observer.foreach(o => o.error(exception))

    def next(value: DataSet): Unit = {
      buffer.enqueue(value)

      if(buffer.size == size)
      {
        _observer.foreach(o => o.next(Dom() ~ Dom(name, null, List(), DataArray(buffer.toList), DataNothing())))
        buffer.clear()
      }
    }

  }

  def subscribe(observer: Observer[Dom]): Unit = {
    _observer.append(observer)
  }


}
