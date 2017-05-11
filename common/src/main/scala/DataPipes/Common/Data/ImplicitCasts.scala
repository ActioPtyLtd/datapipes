package DataPipes.Common.Data

object ImplicitCasts {

  implicit def str2ds(str: String): DataSet = DataString(str)
  implicit def bool2ds(bool: java.lang.Boolean): DataSet = DataBoolean(bool)
  implicit def bool2ds(bool: Boolean): DataSet = DataBoolean(bool)
  implicit def bigDecimal2ds(num: BigDecimal): DataSet = DataNumeric(num)
  implicit def date2ds(date: java.util.Date): DataSet = DataDate(date)

}
