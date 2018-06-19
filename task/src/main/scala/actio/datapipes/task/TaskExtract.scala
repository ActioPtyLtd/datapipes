package actio.datapipes.task

import Term.TermExecutor
import actio.common.Data.{DataArray, DataNothing, DataSet}
import actio.common.{DataSource, Dom, Observer, Task}
import com.typesafe.scalalogging.Logger

import scala.util.Try
import scala.collection.mutable.{ListBuffer, Queue}

class TaskExtract(val name: String, val config: DataSet, val taskSetting: TaskSetting) extends Task {

  private val logger = Logger("TaskExtract")
  private val size: Int = config("size").stringOption.flatMap(m => Try(m.toInt).toOption).getOrElse(100)
  private val dataSource: DataSource = DataSourceFactory(config("dataSource"), taskSetting)
  private val _observer: ListBuffer[Observer[Dom]] = ListBuffer()
  private val buffer = Queue[DataSet]()
  private val termExecutor = new TermExecutor(taskSetting)
  private val termRead = TaskLookup.getTermTree(config("dataSource")("query")("read"))

  def completed(): Unit = {
    _observer.foreach(o => o.completed())
  }

  def error(exception: Throwable): Unit = ???

  lazy val dsObserver = new Observer[DataSet] {
    def completed(): Unit = {
      if (buffer.nonEmpty) {
        logger.info("Sending remaining buffered data...")
        responseAdjust()
        buffer.clear()
        _observer.foreach(o => o.completed())
      }
    }

    def error(exception: Throwable): Unit = _observer.foreach(o => o.error(exception))

    def next(value: DataSet): Unit = {
      buffer.enqueue(value)

      if (buffer.size == size) {
        responseAdjust()
        buffer.clear()
      }
    }

  }

  dataSource.subscribe(dsObserver)

  def next(start: Dom): Unit = {

    if (config("dataSource")("query")("read").toOption.isDefined) {
      start.success.elems.foreach{ e =>
        // if there is paging doesn't evaluate the query, hack!
        val q = if(config("dataSource")("iterate").isDefined) e else TaskLookup.interpolate(termExecutor, termRead, e)
        dataSource.execute(config("dataSource"), q)
      }
    }
    else
      dataSource.execute(config("dataSource"))
  }



  // dont send an array for rest data source if v1
  def responseAdjust(): Unit = {
    if (config("dataSource")("type").stringOption.contains("rest") && taskSetting.version == "v1") {
      val send = for {
        o <- _observer
        b <- buffer
      } yield (o, b)
      send.foreach(s => s._1.next(Dom(name, List(), s._2("root"), DataNothing(), Nil)))
    } else
      _observer.foreach(o => o.next(Dom(name, List(), DataArray(buffer.toList), DataNothing(), Nil)))
  }

  def subscribe(observer: Observer[Dom]): Unit = {
    _observer.append(observer)
  }

}
