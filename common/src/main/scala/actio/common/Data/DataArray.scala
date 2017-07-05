package actio.common.Data

case class DataArray(label: String, arrayElems: List[DataSet]) extends DataBase {

  override def apply(ord: Int): DataSet = arrayElems.lift(ord).getOrElse(DataNothing())

  override def elems: Seq[DataSet] = arrayElems
}

object DataArray {

  private val label = "array"

  def apply(arrayElems: DataSet*): DataArray = new DataArray(label, arrayElems.toList)

  def apply(arrayElems: List[DataSet]): DataArray = new DataArray(label, arrayElems)
}