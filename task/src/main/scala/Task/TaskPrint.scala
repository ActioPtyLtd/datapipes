package Task

import DataPipes.Common._
import DataPipes.Common.Data._
import DataPipes.Common.Data.JsonXmlDataSet.Extend

class TaskPrint(val name: String, config: DataSet) extends DataPipes.Common.Task {
  var _observer: Option[Observer[Dom]] = None

  val formatLookup: Map[String,(DataSet => String)] = Map(
    "raw" -> (ds => ds.print),
    "xml" -> (ds => ds.toXml),
    "json" -> (ds => ds.toJson))

  val format = config("format").stringOption.getOrElse("raw")

  def completed(): Unit= {
    if(_observer.isDefined)
      { _observer.get.completed() }
  }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {

    println(value.headOption.map(s => formatLookup(format)(s.success)).getOrElse(""))

    if(_observer.isDefined)
      { _observer.get.next(value) }
  }

  def subscribe(observer: Observer[Dom]): Unit = {
    _observer = Some(observer)
  }
}
