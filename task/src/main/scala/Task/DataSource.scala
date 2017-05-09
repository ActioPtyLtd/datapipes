package Task

import Common.{DataSet, Parameters}
import DataSources.{CSVDataSource, DBFDataSource, StdInDataSource}

object DataSource {

  private lazy val sources = Map(
    "stdin" -> ((_: DataSet) =>
      new StdInDataSource()),
    "csv" -> ((_: DataSet) =>
      new CSVDataSource()),
    "dbf" -> ((_: DataSet) =>
      new DBFDataSource())
  )


  def apply(parameters: DataSet): Common.DataSource = sources(parameters("type").stringOption.get)(parameters)

}
