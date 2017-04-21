package Data

import Common.DataSet

/**
  * Created by maurice on 21/04/17.
  */
case class DataNothing (label: String) extends DataSetBase

object DataNothing {

  private val label = ""

  def apply(): DataSet = DataNothing(label)
}