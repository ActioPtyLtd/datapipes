package Data

case class DataString(label: String, str: String) extends DataBase {

  def this(_str: String) {
    this ("string",_str)
  }

  override def stringOption: Option[String] = Option(str)
}