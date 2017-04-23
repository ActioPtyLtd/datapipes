/**
  * Created by maurice on 21/04/17.
  */

import Data.{DataNothing}
import DataSources._
import Data.PrettyPrint._
import Common._

object AppTest extends App {

  val src = new CSVDataSource()

  val ds = src.exec(DataNothing())

  val data = ds.data()


  var option: Option[Data] = None

  do {

    option = data.headOption()

    if(option.isDefined)
      Console.println(option.get.print())

  } while(option.isDefined)



}
