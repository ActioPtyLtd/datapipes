package Common.Data

import Common.DataSet

case class DataNothing (label: String) extends DataBase

object DataNothing {

  private val label = ""

  def apply(): DataSet = DataNothing(label)
}