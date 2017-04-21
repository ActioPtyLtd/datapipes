package Data

import Common.Data

/**
  * Created by maurice on 21/04/17.
  */
case class DataNothing (label: String) extends DataBase

object DataNothing {

  private val label = ""

  def apply(): Data = DataNothing(label)
}