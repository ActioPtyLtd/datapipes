package Term

import Common.Data._
import Common.DataSet

object Functions {

  def toUpperCase(str: String): DataSet = DataString(str.toUpperCase)
  def toLowerCase(str: String): DataSet = DataString(str.toLowerCase)
  def trim(str: String): DataSet = DataString(str.trim)

  def substring(str: String, start: Int): DataSet =
    if (start < str.length)
      DataString(str.substring(start))
    else
      DataString("")

  def contains(str: String, targetStr: String): DataSet = DataBoolean(if(str == null || targetStr == null) false else str.contains(targetStr))

}
