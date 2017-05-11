package Task

import DataPipes.Common.Data._
import DataSources.{CSVDataSource, DBFDataSource, JDBCDataSource, StdInDataSource}

object DataSource {

  private lazy val sources = Map(
    "stdin" -> ((_: DataSet) =>
      new StdInDataSource()),
    "csv" -> ((_: DataSet) =>
      new CSVDataSource()),
    "dbf" -> ((_: DataSet) =>
      new DBFDataSource()),
    "jdbc" -> ((_: DataSet) =>
      new JDBCDataSource())
  )


  def apply(parameters: DataSet): DataPipes.Common.DataSource = sources(parameters("type").stringOption.get)(parameters)

}
