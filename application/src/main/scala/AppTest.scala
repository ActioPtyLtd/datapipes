/**
  * Created by maurice on 21/04/17.
  */

import Data.{DataNothing, DataRecord, DataString}
import DataSources._
import Data.PrettyPrint._
import Common._

object AppTest extends App {

  val printer = new Observer[DataEnvelope] {

    override def completed(): Unit = { println("done") }

    override def error(exception: Exception): Unit = ???

    override def next(value: DataEnvelope): Unit = { println(value.success.print())}
  }

  val ds = new CSVDataSource().exec(DataRecord(DataString("filePath", "/home/maurice/gnm/frames_catalogue.csv")))

  ds.subscribe(printer)


}
