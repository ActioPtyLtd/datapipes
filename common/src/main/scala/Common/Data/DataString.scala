package Common.Data

case class DataString(label: String, str: String) extends DataBase {

  override def stringOption: Option[String] = Option(str)
}

object DataString {

  def apply(str: String) = new DataString("string",str)
}