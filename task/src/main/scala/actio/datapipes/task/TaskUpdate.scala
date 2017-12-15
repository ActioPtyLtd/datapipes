package actio.datapipes.task

import Term.TermExecutor
import actio.common.Data.{DataNothing, DataSet}
import actio.common.{Dom, Observer}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Cache {
  var dim: scala.collection.mutable.HashMap[String, String] = mutable.HashMap()

  def clear(): Unit = { dim.clear() }
}

class TaskUpdate(val name: String, val config: DataSet, version: String) extends actio.common.Task {

  private val _observer: ListBuffer[Observer[Dom]] = ListBuffer()
  private val namespace = config("namespace").stringOption.getOrElse("actio.datapipes.task.Term.Functions")
  private val termExecutor = new TermExecutor(namespace)
  private val keyRightTerm = termExecutor.getTemplateTerm(config("keyR").stringOption.getOrElse(""))
  private val changeRightTerm = termExecutor.getTemplateTerm(config("changeR").stringOption.getOrElse(""))
  private val keyLeftTerm = termExecutor.getTemplateTerm(config("keyL").stringOption.getOrElse(""))
  private val changeLeftTerm = termExecutor.getTemplateTerm(config("changeL").stringOption.getOrElse(""))
  private val queryDataSet = TaskLookup.queryAdjust(config("dataSource")("query"), version)
  private val termRead = TaskLookup.getTermTree(queryDataSet("read"))
  private val termCreate = TaskLookup.getTermTree(queryDataSet("create"))
  private val termUpdate = TaskLookup.getTermTree(queryDataSet("update"))

  var initialised = false

  def subscribe(observer: Observer[Dom]): Unit = _observer.append(observer)

  def completed(): Unit = _observer.foreach(o => o.completed())

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {

    if (!initialised) {

      val query = TaskLookup.interpolate(termExecutor, termRead,
        value.success)

      val src = DataSource(config("dataSource"))

      val localObserver = new Observer[DataSet] {

        override def completed(): Unit = {

        }

        override def error(exception: Throwable): Unit = ???

        override def next(value: DataSet): Unit = {
          Cache.dim.put(
            termExecutor.eval(value, keyRightTerm).toString,
            termExecutor.eval(value, changeRightTerm).toString
          )
        }
      }

      src.subscribe(localObserver)

      src.execute(config("dataSource"), query)

      initialised = true

    }

    val incoming = value.success.map(m => (
        m,
        termExecutor.eval(m, keyLeftTerm).toString,
        termExecutor.eval(m, changeLeftTerm).toString
      )).toList
        .groupBy(g => g._2)
        .map(f => f._2.head)
        .toList

    val inserts = incoming.filter(d => Cache.dim.get(d._2).isEmpty)
    val updates = incoming.filter(d => Cache.dim.get(d._2).isDefined)
      .filterNot(d => Cache.dim.get(d._2).contains(d._3))

    if (config("dataSource")("query")("create").toOption.isDefined && inserts.nonEmpty) {
      val src = DataSource(config("dataSource"))

      val query = inserts
        .map(i => TaskLookup.interpolate(termExecutor, termCreate, i._1))

      src.execute(config("dataSource"), query: _*)

      Cache.dim.++=(inserts.map(i => i._2 -> i._3))
    }

    if (config("dataSource")("query")("update").toOption.isDefined && updates.nonEmpty) {
      val src = DataSource(config("dataSource"))

      val query = updates
        .map(i => TaskLookup.interpolate(termExecutor, termUpdate, i._1))

      src.execute(config("dataSource"), query: _*)

      Cache.dim.++=(updates.map(i => i._2 -> i._3))
    }

  }
}
