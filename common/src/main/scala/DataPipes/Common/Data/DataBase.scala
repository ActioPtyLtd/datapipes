package DataPipes.Common.Data

abstract class DataBase extends DataSet {

  def apply(ord: Int): DataSet = DataNothing()

  def apply(field: String): DataSet = DataNothing()

  override def elems: Seq[DataSet] = Seq.empty

  override def toOption: Option[DataSet] = this match {
    case DataNothing(_) => None
    case data => Some(data)
  }
}