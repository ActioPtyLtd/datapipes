package Data

import Common.Data

case class DataRecord(label: String, fields: List[Data]) extends DataBase {

  lazy val mapFields: Map[String, Data] = fields.map(f => f.label -> f).toMap

  override def apply(field: String): Data = mapFields.getOrElse(field, DataNothing())

  override def apply(ord: Int): Data = fields.lift(ord).getOrElse(DataNothing())

  override def elems: Seq[Data] = fields
}

object DataRecord {

  private val label = "record"

  def apply(fields: List[Data]): DataRecord = new DataRecord(label, fields)
  def apply(fields: Data*): DataRecord = new DataRecord(label, fields.toList)
  def apply(label: String, fields: Data*): DataRecord = new DataRecord(label, fields.toList)
}