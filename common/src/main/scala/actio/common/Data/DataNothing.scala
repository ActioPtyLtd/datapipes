package actio.common.Data

case class DataNothing(label: String) extends DataBase {
  override def toString: String = ""
}

object DataNothing {

  private val label = ""

  def apply(): DataSet = DataNothing(label)
}