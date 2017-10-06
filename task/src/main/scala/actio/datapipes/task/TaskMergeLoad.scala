package actio.datapipes.task

import java.text.SimpleDateFormat
import java.util.Date

import actio.common.Data._
import actio.common.{DataSource, Dom, Observer, Task}
import actio.datapipes.dataSources.JDBCDataSource
import actio.datapipes.task.Term.{Functions, TermExecutor}

import scala.collection.mutable.ListBuffer

class TaskMergeLoad(val name: String, val config: DataSet) extends Task {

  private val dataSource: DataSource = DataSource(config("dataSource"))
  private val _observer: ListBuffer[Observer[Dom]] = ListBuffer()
  var retrievedSchema = ""

  def completed(): Unit = {
    _observer.foreach(o => o.completed())
  }

  def error(exception: Throwable): Unit = ???

  def next(value: Dom): Unit = {

    val entity = config("entity").stringOption.getOrElse("")
    val key = if (config("key").stringOption.isDefined) List(config("key").stringOption.getOrElse("")) else
      config("keys").map(_.stringOption.getOrElse("")).toList
    val doUpdate = !config("update").stringOption.contains("false") && key.nonEmpty

    val rows = value.success.elems.groupBy(g => key.map(k => g(k).stringOption.getOrElse(g(k).toString)).mkString("/")).map(_._2.head)

    if(rows.nonEmpty) {

      val cols = rows.toList.flatMap(r => r.elems.map(_.label)).distinct

      if(retrievedSchema.isEmpty)
        retrievedSchema = new JDBCDataSource().getCreateTableStatement(config("dataSource"), "select " +
          cols.map("\"" + _ + "\"").mkString(",") + s" from $entity")



      val tempname = entity.split('.').last

      val createTempTable = s"create temporary table temp_${tempname}($retrievedSchema)"
      val insertHeader = s"insert into temp_${tempname}(" +
        cols.map("\"" + _ + "\"").mkString(",") + ")"

      val insertRow = (r: DataSet) => "(" +
        cols.map(c => TaskMergeLoad.dataSetToInsertValue(r(c))).mkString(",") + ")"

      val insertDest = s"insert into $entity(" +
        cols.map("\"" + _ + "\"").mkString(",") + ")" +
        "select " + cols.map("\"" + _ + "\"").mkString(",") + s" from temp_$tempname" +
        (if(key.isEmpty) "" else
          s" where not exists (select 1 from $entity where " + key.map(k => entity + ".\"" + k + "\" = temp_" + tempname + ".\"" + k + "\"").mkString(" AND ") + ")")

      val updateDest =
        if(doUpdate)
          s"update $entity as td set " +
            cols.map(c => "\"" + c + "\" = ts.\"" + c + "\"").mkString(",") +
            s"from temp_$tempname ts where " + key.map(k => "td.\"" + k + "\" = ts.\"" + k + "\"").mkString(" AND ") + " AND (" +
            cols.map(c => "td.\"" + c + "\" <> ts.\"" + c +
              "\" OR (td.\"" + c + "\" is null AND ts.\"" + c + "\" is not null) OR (td.\"" + c + "\" is not null AND ts.\"" + c + "\" is null)"

            ).mkString(" OR ") + ")"
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
    case DataNothing(_) => "null"
    case str => "'" + Functions.sq(str.stringOption.getOrElse("")) + "'"
  }

}