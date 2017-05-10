package Term.Legacy

import Common.LinkedTree

sealed abstract class SchemaDefinition extends LinkedTree[SchemaDefinition]
{
  def apply(field: String): SchemaDefinition = SchemaUnknown(field)

  def value(keys: List[String]): SchemaDefinition =  keys match {
    case Nil => this
    case lbl::t => this(lbl).value(t)
    case _ => SchemaUnknown }

  def apply(ord: Int): SchemaDefinition = SchemaUnknown

  def toOption: Option[SchemaDefinition] = this match {
    case SchemaUnknown => None
    case schema => Some(schema)
  }

  def unknown = SchemaUnknown

  def schema = this

  override def elems: Seq[SchemaDefinition] = Iterator.empty.toSeq
}

case class SchemaArray(label: String, content: SchemaDefinition) extends SchemaDefinition

object SchemaArray {

  def apply(content: SchemaDefinition) = new SchemaArray("", content)
}

case class SchemaRecord(label: String, fields: List[SchemaDefinition]) extends SchemaDefinition {

  override def apply(field: String): SchemaDefinition = fields.find(f => f.label == field).getOrElse(SchemaUnknown)
}

object SchemaRecord {

  def apply(fields: List[SchemaDefinition]) = new SchemaRecord("", fields)

}

case class SchemaNumber(label: String, precision: Int, scale: Int) extends SchemaDefinition
case class SchemaString(label: String, maxLength: Int) extends SchemaDefinition
case class SchemaDate(label: String, format: String) extends SchemaDefinition
case class SchemaBoolean(label: String) extends SchemaDefinition
case class SchemaUnknown(label: String) extends SchemaDefinition

case object SchemaUnknown extends SchemaDefinition {
  def label = ""
}

sealed abstract class SchemaMatchError
case object SchemaMatchRecordExpected extends SchemaMatchError
case object SchemaMatchArrayExpected extends SchemaMatchError
case object DataDoesntMatchSchema extends SchemaMatchError