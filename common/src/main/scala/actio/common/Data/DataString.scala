package actio.common.Data

case class DataString(label: String, str: String) extends DataBase {

  override def stringOption: Option[String] = Option(str)

  override def apply(ord: Int): DataSet = if(ord < str.length) DataString(str(ord).toString) else DataNothing()

  override def elems: Seq[DataSet] = str.map(s => DataString(s.toString))
}

object DataString {

  def apply(str: String) = new DataString("string", str)
}