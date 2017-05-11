package DataPipes.Common.Data

case class DataNumeric(label: String, num: BigDecimal) extends DataBase {

  override def stringOption: Option[String] = Some(num.toString())
}

object DataNumeric {

  private val name = "numeric"

  def apply(num: BigDecimal): DataNumeric = DataNumeric(name, num)

  def apply(label: String, num: Int): DataNumeric = DataNumeric(label, BigDecimal(num))

  def apply(num: Int): DataNumeric = DataNumeric(name, BigDecimal(num))
}
