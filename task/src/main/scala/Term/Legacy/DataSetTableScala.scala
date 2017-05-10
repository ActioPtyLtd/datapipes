package Term.Legacy

package com.actio

import java.sql.ResultSet
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util

import Common.Data.{DataArray, DataNothing, DataRecord, DataString}
import Common.DataSet

import scala.util.Try
import scala.collection.JavaConverters._


case class DataSetTableScala(myschema: SchemaDefinition, data: DataSet) extends DataSet {
  val rows: List[List[String]] = data.elems.map(_.elems.map(_.stringOption.getOrElse("")).toList).toList
  val header: List[String] = schema.asInstanceOf[SchemaArray].content.asInstanceOf[SchemaRecord].fields.map(_.label).toList

  def this() = this(SchemaUnknown, DataNothing())

  def sizeOfBatch = rows.length

  def getNextBatch: DataSet = this

  def getColumnHeader: util.List[String] = header.asJava

  def getAsListOfColumnsBatch(batchLen: Int): util.List[util.List[String]] = rows.map(_.asJava).asJava

  def getColumnHeaderStr: String = header mkString ","

  def getAsList: util.List[String] = rows.map(r => r.map(Option(_).getOrElse("")) mkString ",").asJava

  def getAsListOfColumns(): util.List[util.List[String]] = rows.map(_.asJava).asJava

  override def toString = (header mkString ", ") + "\n" + ("-" * (header.map(_.length + 2).sum - 2)) + "\n" + (rows map (_ mkString ", ") mkString "\n") + "\n\n" + rows.length + " rows.\n"

  override def elems = List(data)

  def schema = myschema

  def getOrdinalsWithPredicate(predicate: String => Boolean) = header.zipWithIndex filter (c => predicate(c._1)) map (_._2)

  def getColumnValues(columnName: String) = rows map (r => getValue(r, columnName))

  def getValue(row: List[String], columnName: String) = row(getOrdinalOfColumn(columnName))

  def getOrdinalOfColumn(columnName: String) = { val i = header.indexWhere(_ .equalsIgnoreCase(columnName))
    if(i < 0) throw new Exception("Column " + columnName + " doesn't exist")
    i }

  def getNextAvailableColumnName(columnName: String): String = getNextAvailableColumnName(columnName, 1).head

  def getNextAvailableColumnName(columnName: String, n: Int) = {
    val pair = (columnName :: header) map(c => (c.replaceAll("\\d*$", ""), c.reverse takeWhile Character.isDigit match {
      case "" => 1
      case m => m.reverse.toInt + 1
    })) filter(_._1 == columnName.replaceAll("\\d*$", "")) maxBy(_._2)
    (pair._2 until (pair._2 + n)).map(pair._1 + _).toList
  }

  override def label: String = ""

  def apply(field: String): DataSet = ???

  def apply(ord: Int): DataSet = ???

  override def toOption: Option[DataSet] = Some(this)
}

object DataSetTableScala {

  def apply(text: String): DataSetTableScala = apply(List("col1"), List(List(text)))

  def apply(rows2: List[List[String]]): DataSetTableScala = apply(rows2.head.zipWithIndex map ("col" + _), rows2)

  def apply(header: List[String], rows: List[List[String]]): DataSetTableScala = new DataSetTableScala(
    SchemaArray(SchemaRecord(header.map(SchemaString(_,0)).toList)),
    DataArray(rows.map(r => DataRecord((header zip r).map(p => if (p._2 == null) DataNothing(p._1) else DataString(p._1, p._2)).toList)).toList))

  def inPredicate[T](list: List[T]) = (i: T) => list.contains(i)
}
