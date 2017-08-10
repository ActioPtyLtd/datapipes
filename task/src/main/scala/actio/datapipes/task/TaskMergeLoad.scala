package actio.datapipes.task

import java.text.SimpleDateFormat
import java.util.Date

import actio.common.Data._
import actio.common.{DataSource, Dom, Observer, Task}
import actio.datapipes.task.Term.{Functions, TermExecutor}

import scala.collection.mutable.ListBuffer

class TaskMergeLoad(val name: String, val config: DataSet) extends Task {

  private val dataSource: DataSource = DataSource(config("dataSource"))
  private val _observer: ListBuffer[Observer[Dom]] = ListBuffer()

  def completed(): Unit = {
    _observer.foreach(o => o.completed())
  }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {

    val entity = config("entity").stringOption.getOrElse("")
    val key = config("key").stringOption.getOrElse("")
    val doUpdate = !config("update").stringOption.contains("false")

    val rows = value.headOption.map(d => d.success.elems).getOrElse(Seq(DataNothing()))

    if(rows.nonEmpty) {

      val cols = rows.headOption.toList.flatMap(r => r.elems)
      val tempname = entity.split('.').last

      val createTempTable = s"create temporary table temp_${tempname}" +
        "(" + cols.map(c => "\"" + c.label + "\" " + TaskMergeLoad.getColumnType(c)).mkString(",") + ")"
      val insertHeader = s"insert into temp_${tempname}(" +
        cols.map("\"" + _.label + "\"").mkString(",") + ")"

      val insertRow = (r: DataSet) => "(" +
        cols.map(c => TaskMergeLoad.dataSetToInsertValue(r(c.label))).mkString(",") + ")"

      val insertDest = s"insert into $entity(" +
        cols.map("\"" + _.label + "\"").mkString(",") + ")" +
        "select " + cols.map("\"" + _.label + "\"").mkString(",") + s" from temp_$tempname where not exists (select 1 from $entity where $entity.$key = temp_$tempname.$key)"

      val updateDest =
        if(doUpdate)
          s"update $entity as td set " +
            cols.map(c => "\"" + c.label + "\" = ts.\"" + c.label + "\"").mkString(",") +
            s"from temp_$tempname ts where td.$key = ts.$key AND (" +
            cols.map(c => "td.\"" + c.label + "\" <> ts.\"" + c.label + "\"").mkString(" OR ") + ")"
        else ""


      val query = s"$createTempTable;$insertHeader values ${rows.map(insertRow).mkString(",")};$insertDest;${updateDest}"


      dataSource.execute(config("dataSource"), DataString("create", query))
    }
  }

  def subscribe(observer: Observer[Dom]): Unit = {
    _observer.append(observer)
  }

}

object TaskMergeLoad {
  def dataSetToInsertValue(ds: DataSet): String = ds match {
    case DataNumeric(_,num) => num.toString
    case DataBoolean(_,bool) => bool.toString
    case DataDate(_,date) => "'" + new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date(date)) + "'"
    case str => "'" + Functions.sq(str.stringOption.getOrElse("")) + "'"
  }
  def getColumnType(ds: DataSet): String = ds match {
    case DataNumeric(_,_) => "numeric"
    case DataBoolean(_,_) => "boolean"
    case DataDate(_,date) => "timestamp"
    case _ => "varchar"
  }
}