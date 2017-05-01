package Common.Data

import Common.DataSet

case class DataRecord(label: String, fields: List[DataSet]) extends DataBase {

  lazy val mapFields: Map[String, DataSet] = fields.map(f => f.label -> f).toMap

  override def apply(field: String): DataSet = mapFields.getOrElse(field, DataNothing())

  override def apply(ord: Int): DataSet = fields.lift(ord).getOrElse(DataNothing())

  override def elems: Seq[DataSet] = fields
}

object DataRecord {

  private val label = "record"

  def apply(fields: List[DataSet]): DataRecord = new DataRecord(label, fields)
  def apply(fields: DataSet*): DataRecord = new DataRecord(label, fields.toList)
  def apply(label: String, fields: DataSet*): DataRecord = new DataRecord(label, fields.toList)
}