package Data

/**
  * Created by maurice on 21/04/17.
  */
case class DataNothing (label: String) extends DataSet

object DataNothing {

  private val label = ""

  def apply(): DataSet = DataNothing(label)
}