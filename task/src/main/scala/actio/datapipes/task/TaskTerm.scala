package actio.datapipes.task

import Term.TermExecutor
import actio.common.Data.{DataNothing, DataSet}
import actio.common.Dom

import scala.meta._

class TaskTerm(name: String, config: DataSet, version: String) extends TaskTransform(name) {

  val term: Term = config("term").stringOption.getOrElse("").parse[Term].get
  val executor = new TermExecutor(config("namespace").stringOption.getOrElse("actio.datapipes.task.Term.Legacy.Functions"))

  def transform(dom: Dom): Seq[DataSet] = {
    val ds = executor.eval(dom.headOption.map(_.success).getOrElse(DataNothing()), term)

    if (version.contains("v2"))
      List(ds)
    else
      ds.elems
  }

}
