package Common.Data

abstract class DataBase extends Data {

  def apply(ord: Int): Data = DataNothing()

  def apply(field: String): Data = DataNothing()

  override def elems: Seq[Data] = Seq.empty

  override def toOption: Option[Data] = this match {
    case DataNothing(_) => None
    case data => Some(data)
  }

  def map[B](f:Data => B): Seq[B] = elems.map(f)

}