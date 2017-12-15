package actio.common.Data

case class DataBoolean(label: String, bool: Boolean) extends DataBase {

  override def stringOption: Option[String] = Some(bool.toString)

  override def toString: String = bool.toString
}

object DataBoolean {

  private val label = "bool"

  def apply(bool: Boolean): DataBoolean = DataBoolean(label, bool)
}
