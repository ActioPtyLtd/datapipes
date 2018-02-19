package actio.datapipes.task

import actio.common.Data.{DataBoolean, DataSet}
import actio.common.DataSource
import actio.datapipes.dataSources._
import actio.datapipes.task.Term.TermExecutor

import scala.meta.Term
import scala.meta._


object DataSourceFactory {

  private lazy val sources = Map(
    "stdin" -> ((_: DataSet) =>
      new StdInDataSource()),
    "file" -> ((config: DataSet) => {
      val behavior = config("behavior").stringOption

      if(behavior.contains("DBF"))
        new LocalFileSystemDataSource("dbf")
      else if(behavior.contains("csv"))
        new LocalFileSystemDataSource("csv")
      else if(behavior.contains("json"))
        new LocalFileSystemDataSource("json")
      else if(behavior.contains("dump"))
        new LocalFileSystemDataSource("dump")
      else if(behavior.contains("txt"))
        new LocalFileSystemDataSource("txt")
      else
        new TextFileDataSource()
    }),
    "zip" -> ((_: DataSet) => new ZipDataSource()),
    "ftp" -> ((config: DataSet) => {
      val behavior = config("behavior").stringOption
        new FTPDataSource(behavior.getOrElse(throw new UnsupportedOperationException(s"behavior required for data source.")))
    }),
    "txt" -> ((_: DataSet) =>
      new LocalFileSystemDataSource("txt")),
    "rest" -> ((_: DataSet) =>
      new RESTJsonDataSource()),
    "s3" -> ((_: DataSet) =>
      new AWSS3DataSource()),
    "sql" -> ((_: DataSet) =>
      new JDBCDataSource())
  )


  def apply(parameters: DataSet): DataSource = {

    val iterateOption = parameters("iterate")("until").stringOption

    val dataSource = sources(parameters("type").stringOption.get)(parameters)

    lazy val termExecutor = new TermExecutor("actio.datapipes.task.Term.Functions")
    lazy val termRead = TaskLookup.getTermTree(parameters("query")("read"))
    lazy val termUntil = parameters("iterate")("until").stringOption.get.parse[Term].get

    iterateOption
      .map(i => new PagingDataSource(parameters, dataSource,ds =>
        TaskLookup.interpolate(termExecutor,termRead,ds), ds => {
          val e = termExecutor.eval(ds,termUntil)
          e match {
          case DataBoolean(_,t) => t
          case _ => true
        }
      }  ))
      .getOrElse(dataSource)
  }

}
