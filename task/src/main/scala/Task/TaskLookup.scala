package Task

import DataPipes.Common.Data._
import DataPipes.Common.{DataSource, Dom, Observer, Task}
import Term.TermExecutor

import scala.collection.mutable.ListBuffer
import scala.meta._
import scala.meta.Term

class TaskLookup(name: String, config: DataSet, version: String) extends Task {

  private val _observer: ListBuffer[Observer[Dom]] = ListBuffer()
  private val terms = TaskLookup.getTermTree(TaskLookup.queryAdjust(config("dataSource")("query")("read"), version))
  private val namespace = config("namespace").stringOption.getOrElse("Term.Legacy.Functions")
  private val termExecutor = new TermExecutor(namespace)

  def completed(): Unit = {
    _observer.foreach(o => o.completed())
  }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {

    val dataSource: DataSource = DataSource(config("dataSource"))
    val ret = ListBuffer[DataSet]()

    val localObserver = new Observer[DataSet] {

      var singleBuffer = ListBuffer[DataSet]()

      override def completed(): Unit = {
        val nds = singleBuffer.headOption match {
          case Some(_: DataArray) => DataArray(singleBuffer.flatMap(m => m.elems).toList)
          case Some(ds: DataSet) if singleBuffer.size == 1 => ds
        }
        ret.append(nds)
        singleBuffer.clear()
      }

      override def error(exception: Throwable): Unit = ???

      override def next(value: DataSet): Unit = {
        singleBuffer.append(value)
      }
    }

    dataSource.subscribe(localObserver)

    value.headOption.foreach { h =>
      h.success.elems.foreach { e =>
        dataSource.execute(config("dataSource"), TaskLookup.interpolate(termExecutor, terms, e))
      }
    }

    // TODO: check old code for merge logic
    val merge = value.headOption.map(h => DataArray((h.success.elems zip ret).map(z =>
      DataRecord(DataArray(name, z._2 :: z._1.elems.toList))
    ).toList)).getOrElse(DataNothing())

    _observer.foreach(o => o.next(value ~ Dom(name, Nil, merge, DataNothing())))
  }

  def subscribe(observer: Observer[Dom]): Unit = _observer.append(observer)
}

object TaskLookup {

  def interpolate(termExecutor: TermExecutor, termTree: TermLinkedTree, ds: DataSet): DataSet = termTree match {
    case node: TermNode => DataRecord(node.label, node.fields.map(f => interpolate(termExecutor, f, ds)))
    case leaf: TermLeaf => DataString(leaf.label, termExecutor.eval(ds, leaf.term).stringOption.getOrElse(""))
  }

  def getTermTree(config: DataSet): TermLinkedTree = config match {
    case DataString(label, str) => TermLeaf(label, ("s\"\"\"" + str + "\"\"\"").parse[Term].get) //TODO: fix no parse
    case DataRecord(label, fields) =>
      TermNode(
        label,
        fields
          .filter(f => f.isInstanceOf[DataRecord] || f.isInstanceOf[DataString])
          .map(m => getTermTree(config(m.label)))
      )
    case _ => TermLeaf("", Term.Name("a").asInstanceOf[Term])
  }

  // add a $ sign to any templates if it looks like a template variable if v1
  // except if label is verb
  def queryAdjust(query: DataSet, version: String): DataSet =
    if (version == "v1") {
      query match {
        case r: DataRecord => DataRecord(r.label, r.fields.map(f => queryAdjust(f, version)))
        case DataString(label, str) if str.matches("[a-zA-Z_]((-)?[a-zA-Z_0-9])*$") && label != "verb" => DataString(label, "$" + str)
        case ds => ds
      }
    } else
      query
}