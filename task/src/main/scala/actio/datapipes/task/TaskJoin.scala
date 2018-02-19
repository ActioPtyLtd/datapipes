package actio.datapipes.task

import Term.TermExecutor
import actio.common.Data.{DataArray, DataNothing, DataRecord, DataSet}
import actio.common.{Dom, Observer, Task}

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.meta._

class TaskJoin(val name: String, val config: DataSet, version: String) extends Task {

  private val _observer: ListBuffer[Observer[Dom]] = ListBuffer()
  private val namespace = config("namespace").stringOption.getOrElse("actio.datapipes.task.Term.Functions")
  private val termExecutor = new TermExecutor(namespace)
  private val keyRightTerm = termExecutor.getTemplateTerm(config("keyR").stringOption.getOrElse(""))
  private val keyLeftTerm = termExecutor.getTemplateTerm(config("keyL").stringOption.getOrElse(""))
  private val iterateRightTerm: Option[Term] = config("iterateR").stringOption.map(m => m.parse[Term].get)
  private val termRead = config("dataSource")("query")("read").toOption.map(r => TaskLookup.getTermTree(r))
  private val lookup = HashMap[String, DataSet]()

  var initialised = false

  def subscribe(observer: Observer[Dom]): Unit = _observer.append(observer)

  def completed(): Unit = _observer.foreach(o => o.completed())

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {

    if (!initialised) {

      val query = termRead.map(r => TaskLookup.interpolate(termExecutor, r,
        value.success)).getOrElse(DataNothing())

      val src = DataSourceFactory(config("dataSource"))

      val localObserver = new Observer[DataSet] {

        override def completed(): Unit = {

        }

        override def error(exception: Throwable): Unit = ???

        // total hack
        val adjustForREST: Boolean = (config("dataSource")("type").stringOption.contains("rest") ||
          config("dataSource")("type").stringOption.contains("file") ) && version == "v1"

        override def next(value: DataSet): Unit = {

          if(iterateRightTerm.isDefined)
            {
              iterateRightTerm.foreach { t =>

                (if(adjustForREST)
                  termExecutor.eval(value, t).map(i => (
                    termExecutor.eval(i, keyRightTerm).toString,
                    i
                  ))
                else {
                  val i = termExecutor.eval(value, t)
                  List((termExecutor.eval(i, keyRightTerm).toString,i))
                })
                .foreach { f =>
                  lookup.put(f._1, f._2)
                }
              }
            }
          else
            lookup.put(
              termExecutor.eval(value, keyRightTerm).toString,
              value
            )
        }
      }

      src.subscribe(localObserver)

      src.execute(config("dataSource"), query)

      initialised = true
    }

    val incoming = value.success.map(m =>
        termExecutor.eval(m, keyLeftTerm).stringOption.map(kLeft => lookup.get(kLeft).map(r =>
          DataRecord(m.label, DataRecord(name, List(r)) :: m.elems.toList)).getOrElse(
          DataRecord(m.label, DataNothing(name) :: m.elems.toList)
        )).getOrElse(m)).toList

    _observer.foreach(o => o.next(Dom(name, Nil, DataArray(incoming), DataNothing(), Nil)))
  }
}
