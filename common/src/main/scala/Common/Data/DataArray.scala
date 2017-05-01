package Common.Data

case class DataArray(label: String, arrayElems: List[Data]) extends DataBase {

  override def apply(ord: Int): Data = arrayElems.lift(ord).getOrElse(DataNothing())

  override def elems: Seq[Data] = arrayElems
}

object DataArray {

  private val label = "array"

  def apply(arrayElems: Data*): DataArray = new DataArray(label, arrayElems.toList)
  def apply(arrayElems: List[Data]): DataArray = new DataArray(label, arrayElems)
}