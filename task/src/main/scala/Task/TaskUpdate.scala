package Task

import DataPipes.Common.Data._
import DataPipes.Common.{DataSource, Dom, Observer, Task}
import Term.TermExecutor

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Cache {
  var dim: scala.collection.mutable.HashMap[String, String] = mutable.HashMap()

  def clear: Unit = { dim.clear() }
}

class TaskUpdate(val name: String, val config: DataSet) extends Task {

  val _observer: ListBuffer[Observer[Dom]] = ListBuffer()
  val namespace = config("namespace").stringOption.getOrElse("Term.Functions")
  val termExecutor = new TermExecutor(namespace)
  val keyRightTerm = termExecutor.getTemplateTerm(config("keyR").stringOption.getOrElse(""))
  val changeRightTerm = termExecutor.getTemplateTerm(config("changeR").stringOption.getOrElse(""))
  val keyLeftTerm = termExecutor.getTemplateTerm(config("keyL").stringOption.getOrElse(""))
  val changeLeftTerm = termExecutor.getTemplateTerm(config("changeL").stringOption.getOrElse(""))
  val terms = TaskLookup.getTermTree(config("dataSource")("query"))
  var initialised = false

  def subscribe(observer: Observer[Dom]): Unit = _observer.append(observer)

  def completed(): Unit = ???

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {

    if(!initialised) {

      val newConfig = Operators.mergeLeft(DataRecord("dataSource", TaskLookup.interpolate(termExecutor, terms,
        value.headOption.map(m => m.success).getOrElse(DataNothing()))), config("dataSource"))

      val src = DataSource(config("dataSource"))

      val localObserver = new Observer[DataSet] {

        override def completed(): Unit = {

        }

        override def error(exception: Throwable): Unit = ???

        override def next(value: DataSet): Unit = {
          Cache.dim.put(
            termExecutor.eval(value, keyRightTerm).stringOption.getOrElse(""),
            termExecutor.eval(value, changeRightTerm).stringOption.getOrElse(""))
        }
      }

      src.subscribe(localObserver)

      src.exec(newConfig)

      initialised = true

    }

    val incoming = value
      .headOption
      .toList
      .flatMap(_.success.map(m => (
        m,
        termExecutor.eval(m, keyLeftTerm).stringOption.getOrElse(""),
        termExecutor.eval(m, changeLeftTerm).stringOption.getOrElse("")
      )).toList
      .groupBy(g => g._2)
      .map(f => f._2.head)
      .toList)


    val inserts = incoming.filter(d => Cache.dim.get(d._2).isEmpty)
    val updates = incoming.filter(d => Cache.dim.get(d._2).isDefined)
      .filterNot(d => Cache.dim.get(d._2).contains(d._3))

    if(config("dataSource")("query")("create").toOption.isDefined) {
      val src = DataSource(config("dataSource"))
      src.exec(DataArray(inserts.map(_._1)))
    }


  }
}
