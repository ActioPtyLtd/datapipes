package Task
import DataPipes.Common.Data._
import DataPipes.Common.{DataSource, Dom, Observer, Task}
import Term.TermExecutor

import scala.concurrent.Future
import scala.meta._
import scala.meta.Term
import scala.async.Async.{async, await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class TaskLookup(name: String, config: DataSet) extends Task {

  var _observer: Option[Observer[Dom]] = None
  val terms = TaskLookup.getTermTree(config("dataSource")("query"))
  val termExecutor = new TermExecutor("Term.Function")

  def completed(): Future[Unit] = async {
    if(_observer.isDefined)
      await { _observer.get.completed()}
  }

  def error(exception: Throwable): Future[Unit] = ???

  def next(value: Dom): Future[Unit] = async {

    val newConfig = Operators.mergeLeft(DataRecord("dataSource", TaskLookup.interpolate(termExecutor, terms,
      value.headOption.map(m => m.success).getOrElse(DataNothing()))), config("dataSource"))

    val dataSource: DataSource = DataSource(config("dataSource"))

    val localObserver = new Observer[DataSet] {

      var buffer = List[DataSet]()

      override def completed(): Future[Unit] = async {
        val nds = buffer.headOption match {
          case Some(_: DataArray) => DataArray(buffer.flatMap(m => m.elems))
          case Some(ds: DataSet) if buffer.size == 1 => ds
        }

        if(_observer.isDefined)
          await {

            _observer.get.next(Dom() ~ Dom(name, null, Nil, nds, DataNothing()))
          }
      }

      override def error(exception: Throwable): Future[Unit] = ???

      override def next(value: DataSet): Future[Unit] = async {
        buffer = value :: buffer
      }
    }

    dataSource.subscribe(localObserver)

    await { dataSource.exec(newConfig) }
  }

  def subscribe(observer: Observer[Dom]): Unit = _observer = Some(observer)
}

object TaskLookup {

  def interpolate(termExecutor: TermExecutor, termTree: TermLinkedTree, ds: DataSet): DataSet = termTree match {
    case node: TermNode => DataRecord(node.label, node.fields.map(f => interpolate(termExecutor, f, ds)))
    case leaf: TermLeaf => DataString(leaf.label, termExecutor.eval(ds, leaf.term).stringOption.getOrElse(""))
  }

  def getTermTree(config: DataSet): TermLinkedTree = config match {
    case DataString(label, str) => TermLeaf(label, ("s\"\"\"" + str + "\"\"\"").parse[Term].get ) //TODO: fix no parse
    case DataRecord(label, fields) =>
      TermNode(label,
        fields
          .filter(f => f.isInstanceOf[DataRecord] || f.isInstanceOf[DataString])
          .map(m => getTermTree(config(m.label))))
      // TODO: handle bad use
  }
}