package actio.datapipes.task

import Term.TermExecutor
import actio.common.Data._
import actio.common.{DataSource, Dom, Observer, Task}

import scala.collection.mutable.ListBuffer
import scala.meta._

class TaskLookup(name: String, config: DataSet, version: String) extends Task {

  private val _observer: ListBuffer[Observer[Dom]] = ListBuffer()
  private val terms = TaskLookup.getTermTree(TaskLookup.queryAdjust(config("dataSource")("query")("read"), version))
  private val namespace = config("namespace").stringOption.getOrElse("actio.datapipes.task.Term.Legacy.Functions")
  private val termExecutor = new TermExecutor(namespace)

  def completed(): Unit = {
    _observer.foreach(o => o.completed())
  }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {

    val dataSource: DataSource = DataSourceFactory(config("dataSource"))
    val ret = ListBuffer[DataSet]()

    val localObserver = new Observer[DataSet] {

      var singleBuffer = ListBuffer[DataSet]()

      override def completed(): Unit = {
        val nds = singleBuffer.headOption match {
          case Some(_: DataArray) => DataArray(singleBuffer.flatMap(m => m.elems).toList)
          case Some(ds: DataSet) if singleBuffer.size == 1 => ds
          case Some(ds: DataSet) => DataRecord(DataArray(singleBuffer.toList))
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

    value.success.elems.foreach { e =>
      config("waiting").intOption.foreach(t => Thread.sleep(t))
      dataSource.execute(config("dataSource"), TaskLookup.interpolate(termExecutor, terms, e))
    }


    // TODO: check old code for merge logic
    val merge = DataArray((value.success.elems zip ret).map(z =>

      if(version == "v1")
        DataRecord(DataArray(name,z._2.elems.toList) :: z._1.elems.toList)
      else
        Operators.mergeLeft(z._1, z._2)
    ).toList)

    _observer.foreach(o => o.next(Dom(name, Nil, merge, DataNothing(), Nil)))
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
    case _ => TermLeaf("", scala.meta.Term.Name("a").asInstanceOf[Term])
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