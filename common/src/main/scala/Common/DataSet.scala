package Common

trait DataSet {
  def data(): SimpleIterator[Data]
}
