/**
  * Created by maurice on 21/04/17.
  */

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import Data.{DataNothing, DataRecord, DataString}
import DataSources._
import Data.PrettyPrint._
import Common._

object AppTest extends App {

  val printer = new Observer[DataEnvelope] {

    override def completed(): Future[Unit]= Future { println("done") }

    override def error(exception: Throwable): Future[Unit] = ???

    override def next(value: DataEnvelope): Future[Unit] = Future { println(value.success.toXml) }
  }

  val src = new CSVDataSource().run(printer, DataRecord(DataString("filePath", "/home/maurice/gnm/frames_catalogue.csv")))

  val test  = DataRecord("top", DataRecord("attributes", DataString("a1", "v1"), DataString("a2", "v2")),DataString("e1", "v3"))
  println(test.toXml)

  Await.result(src, 1000 millis)

}
