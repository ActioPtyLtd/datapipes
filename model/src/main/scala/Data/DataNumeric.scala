package Data

case class DataNumeric(label: String, num: BigDecimal) extends DataSetBase {

  override def stringOption: Option[String] = Some(num.toString())
}

object DataNumeric {

  private val name = "numeric"

  def apply(num: BigDecimal): DataNumeric = DataNumeric(name, num)

  def apply(num: Int): DataNumeric = DataNumeric(name, BigDecimal(num))
}
