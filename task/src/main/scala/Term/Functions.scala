package Term

import java.lang._
import java.text.{DecimalFormat, SimpleDateFormat}

import scala.util.Try

object Functions {

  def toUpperCase(str: String): String = str.toUpperCase
  def toLowerCase(str: String): String = str.toLowerCase
  def trim(str: String): String = str.trim

  def substring(str: String, start: Int): String =
    if (start < str.length)
      str.substring(start)
    else
      ""

  def contains(str: String, targetStr: String): Boolean =
    if(str == null || targetStr == null)
      false
    else
      str.contains(targetStr)

  def numeric(str: String, default: BigDecimal): BigDecimal = Try(BigDecimal(str)).getOrElse(default)
  def numeric(str: String): BigDecimal = numeric(str, 0)
  def numeric(str: String, format: String): String = new DecimalFormat(format).format(numeric(str))

  def date(date: java.util.Date, format: String): String = new SimpleDateFormat(format).format(date)

  // short hands
  def sq(str: String): String = str.replace("'","''")

}
