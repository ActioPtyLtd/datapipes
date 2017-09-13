package actio.datapipes.task.Term

import scala.meta._
import actio.common.Data._
/**
  * Created by mauri on 31/08/2016.
  */
object MetaTermTest extends App {

  val ds = DataRecord("", List(DataString("key", "abc"), DataDate("key2", new java.util.Date(2000 - 1900, 1, 1))))
  val text = "ds => ds.key + \"def\""

  val term = text.parse[Term]

  println(term.get.structure)

  val res = new TermExecutor("").eval(ds, term.get)

  println(res)
}
