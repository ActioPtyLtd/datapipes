package actio.datapipes.task

import actio.common.Data.DataSet
import actio.common.DataSource
import actio.datapipes.dataSources._


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


  def apply(parameters: DataSet): DataSource = sources(parameters("type").stringOption.get)(parameters)

}
