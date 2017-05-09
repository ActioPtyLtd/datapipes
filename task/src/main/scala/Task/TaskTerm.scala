package Task


import Common.Data.DataNothing
import Common._
import Term.TermExecutor


import scala.meta.Term

class TaskTerm(name: String, val term: Term) extends TaskTransform(name) {

  def transform(dom: Dom): Parameters = TermExecutor.eval(dom.headOption.map(_.success).getOrElse(DataNothing()), term)

}
