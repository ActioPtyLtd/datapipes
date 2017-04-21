package Data

import Common.DataSet

abstract class DataSetBase extends DataSet {

  def apply(ord: Int): DataSet = DataNothing()

  def apply(field: String): DataSet = DataNothing()

  override def elems: Seq[DataSet] = Seq.empty

  override def toOption: Option[DataSet] = this match {
    case DataNothing(_) => None
    case data => Some(data)
  }

  def map[B](f:DataSet => B): Seq[B] = elems.map(f)

}