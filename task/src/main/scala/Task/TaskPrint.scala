package Task

import DataPipes.Common._
import DataPipes.Common.Data._
import DataPipes.Common.Data.JsonXmlDataSet.Extend
import com.typesafe.scalalogging.Logger

import scala.collection.mutable.ListBuffer

class TaskPrint(val name: String, config: DataSet) extends DataPipes.Common.Task {
  private val logger = Logger("TaskPrint")
  private val _observer: ListBuffer[Observer[Dom]] = ListBuffer()
  private val formatLookup: Map[String, (DataSet => String)] = Map(
    "raw" -> (ds => ds.print),
    "xml" -> (ds => ds.toXml),
    "json" -> (ds => ds.toJson)
  )
  private val format: String = config("format").stringOption.getOrElse("raw")

  def completed(): Unit = {
    _observer.foreach(o => o.completed())
  }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {

    logger.info(value.headOption.map(s => formatLookup(format)(s.success)).getOrElse(""))

    _observer.foreach(o => o.next(value))
  }

  def subscribe(observer: Observer[Dom]): Unit = {
    _observer.append(observer)
  }
}