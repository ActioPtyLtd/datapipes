package Task


import Common.Data.DataNothing
import Common._
import Term.TermExecutor
import scala.meta._

import scala.meta.Term

class TaskTerm(name: String, config: DataSet) extends TaskTransform(name) {

  val term: Term = config("term").stringOption.getOrElse("").parse[Term].get
  val executor = new TermExecutor(config("namespace").stringOption.getOrElse("Term.Functions"))

  def transform(dom: Dom): Parameters = executor.eval(dom.headOption.map(_.success).getOrElse(DataNothing()), term)

}
