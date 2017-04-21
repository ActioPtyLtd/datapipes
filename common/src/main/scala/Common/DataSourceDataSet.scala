package Common

class DataSourceDataSet(src: DataSource) extends DataSet {

  override def data: SimpleIterator[Data] = new SimpleIterator[Data] {
    override def headOption(): Option[Data] = src.headOption()
  }
}