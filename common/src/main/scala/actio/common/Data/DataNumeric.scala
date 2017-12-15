package actio.common.Data

case class DataNumeric(label: String, num: BigDecimal) extends DataBase {

  override def stringOption: Option[String] = Some(num.toString)

  override def toString: String = num.toString

  override def intOption: Option[Int] = if(num.isValidInt) Some(num.toInt) else None
}

object DataNumeric {

  private val name = "numeric"

  def apply(num: BigDecimal): DataNumeric = DataNumeric(name, num)

  def apply(label: String, num: Int): DataNumeric = DataNumeric(label, BigDecimal(num))

  def apply(num: Int): DataNumeric = DataNumeric(name, BigDecimal(num))
}
