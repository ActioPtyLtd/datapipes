package Task

import DataPipes.Common.Data._
import DataSources._

object DataSource {

  private lazy val sources = Map(
    "stdin" -> ((_: DataSet) =>
      new StdInDataSource()),
    "file" -> ((config: DataSet) =>
      if(config("behavior").stringOption.contains("DBF"))
        new DBFDataSource()
      else new CSVDataSource()),
    "rest" -> ((_: DataSet) =>
      new RESTJsonDataSource()),
    "sql" -> ((_: DataSet) =>
      new JDBCDataSource())
  )


  def apply(parameters: DataSet): DataPipes.Common.DataSource = sources(parameters("type").stringOption.get)(parameters)

}
