/**
  * Created by maurice on 21/04/17.
  */

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import Common.Data.{DataNothing, DataRecord, DataString}
import DataSources.{CSVDataSource}
import Common.Data.PrettyPrint._
import Common._

object AppTest extends App {

  val printer = new Observer[DataSet] {

    override def completed(): Future[Unit]= Future { println("done") }

    override def error(exception: Throwable): Future[Unit] = ???

    override def next(value: DataSet): Future[Unit] = Future { println(value.toXml) }
  }

  val src = new CSVDataSource()
  src.subscribe(printer)
  var f = src.exec(DataRecord(DataString("filePath", "/home/maurice/gnm/frames_catalogue.csv")))

  Await.result(f, 1000 millis)

}
