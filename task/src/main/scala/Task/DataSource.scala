package Task

import DataPipes.Common.Data._
import DataSources._

object DataSource {

  private lazy val sources = Map(
    "stdin" -> ((_: DataSet) =>
      new StdInDataSource()),
    "file" -> ((config: DataSet) => {
      val behavior = config("behavior").stringOption

      if(behavior.contains("DBF"))
        new DBFDataSource()
      else if(behavior.contains("csv"))
        new CSVDataSource()
      else if(behavior.contains("dump"))
        new DumpDataSource()
      else
        new TextFileDataSource()
    }),
    "rest" -> ((_: DataSet) =>
      new RESTJsonDataSource()),
    "sql" -> ((_: DataSet) =>
      new JDBCDataSource())
  )


  def apply(parameters: DataSet): DataPipes.Common.DataSource = sources(parameters("type").stringOption.get)(parameters)

}
