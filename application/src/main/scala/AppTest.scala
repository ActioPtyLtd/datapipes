/**
  * Created by maurice on 21/04/17.
  */

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import Common.Data.{DataNothing, DataRecord, DataString}
import DataSources.{CSVDataSource, StdInDataSource}
import Common.Data.PrettyPrint._
import Common._

object AppTest extends App {

  val printer = new Observer[Dom] {

    def completed(): Future[Unit]= Future { println("done") }

    def error(exception: Throwable): Future[Unit] = ???

    def next(value: Dom): Future[Unit] = Future { println(value.success) }
  }

  val t = Task("TaskExtract")

  t.subscribe(printer)
  //var f = src.exec(DataRecord(DataString("filePath", "/home/maurice/gnm/frames_catalogue.csv")))

  var f = t.next(DataNothing())

  Await.result(f, 100000 millis)

}
