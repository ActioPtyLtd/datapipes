package Term

import java.lang._

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

}
