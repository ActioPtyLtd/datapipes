package actio.datapipes.task

import Term.TermExecutor
import actio.common.Data.{DataArray, DataNothing, DataSet}
import actio.common.{DataSource, Dom, Observer, Task}
import com.typesafe.scalalogging.Logger

import scala.util.Try
import scala.collection.mutable.{ListBuffer, Queue}

class TaskExtract(val name: String, val config: DataSet, val version: String) extends Task {

  private val logger = Logger("TaskExtract")
  private val size: Int = config("size").stringOption.flatMap(m => Try(m.toInt).toOption).getOrElse(100)
  private val dataSource: DataSource = DataSource(config("dataSource"))
  private val _observer: ListBuffer[Observer[Dom]] = ListBuffer()
  private val buffer = Queue[DataSet]()
  private val namespace = config("namespace").stringOption.getOrElse("actio.datapipes.task.Term.Legacy.Functions")
  private val termExecutor = new TermExecutor(namespace)
  private val termRead = TaskLookup.getTermTree(config("dataSource")("query")("read"))

  def completed(): Unit = { Unit }

  def error(exception: Throwable): Unit = ???

  def next(start: Dom): Unit = {

    lazy val dsObserver = new Observer[DataSet] {
      def completed(): Unit = {
        if (buffer.nonEmpty) {
          logger.info("Sending remaining buffered data...")
          responseAdjust(start)
          buffer.clear()
          _observer.foreach(o => o.completed())
        }
      }

      def error(exception: Throwable): Unit = _observer.foreach(o => o.error(exception))

      def next(value: DataSet): Unit = {
        buffer.enqueue(value)

        if (buffer.size == size) {
          responseAdjust(start)
          buffer.clear()
        }
      }

    }

    dataSource.subscribe(dsObserver)

    if (config("dataSource")("query")("read").toOption.isDefined)
      dataSource.execute(config("dataSource"), TaskLookup.interpolate(termExecutor, termRead,
        start.headOption.map(m => m.success).getOrElse(DataNothing())))
    else
      dataSource.execute(config("dataSource"))
  }



  // dont send an array for rest data source if v1
  def responseAdjust(start: Dom): Unit = {
    if (config("dataSource")("type").stringOption.contains("rest") && version == "v1") {
      val send = for {
        o <- _observer
        b <- buffer
      } yield (o, b)
      send.foreach(s => s._1.next(start ~ Dom(name, List(), s._2("root"), DataNothing(), Nil)))
    } else
      _observer.foreach(o => o.next(start ~ Dom(name, List(), DataArray(buffer.toList), DataNothing(), Nil)))
  }

  def subscribe(observer: Observer[Dom]): Unit = {
    _observer.append(observer)
  }

}
