package Task
import DataPipes.Common.Data._
import DataPipes.Common.{DataSource, Dom, Observer, Task}
import Term.TermExecutor

import scala.meta._
import scala.meta.Term

class TaskLookup(name: String, config: DataSet) extends Task {

  var _observer: Option[Observer[Dom]] = None
  val terms = TaskLookup.getTermTree(config("dataSource")("query"))
  val namespace = config("namespace").stringOption.getOrElse("Term.Functions")
  val termExecutor = new TermExecutor(namespace)

  def completed(): Unit = {
    if(_observer.isDefined)
      _observer.get.completed()
  }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {

    val newConfig = Operators.mergeLeft(DataRecord("dataSource", TaskLookup.interpolate(termExecutor, terms,
      value.headOption.map(m => m.success).getOrElse(DataNothing()))), config("dataSource"))

    val dataSource: DataSource = DataSource(config("dataSource"))

    val localObserver = new Observer[DataSet] {

      var buffer = List[DataSet]()

      override def completed(): Unit = {
        val nds = buffer.headOption match {
          case Some(_: DataArray) => DataArray(buffer.flatMap(m => m.elems))
          case Some(ds: DataSet) if buffer.size == 1 => ds
        }

        if(_observer.isDefined)
        {
          _observer.get.next(Dom() ~ Dom(name, null, Nil, nds, DataNothing()))
        }
      }

      override def error(exception: Throwable): Unit = ???

      override def next(value: DataSet): Unit = {
        buffer = value :: buffer
      }
    }

    dataSource.subscribe(localObserver)

    dataSource.exec(newConfig)
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