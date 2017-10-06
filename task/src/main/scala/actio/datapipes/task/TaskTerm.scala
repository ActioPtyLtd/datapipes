package actio.datapipes.task

import Term.TermExecutor
import actio.common.Data.{DataArray, DataNothing, DataSet}
import actio.common.Dom

import scala.meta._

class TaskTerm(name: String, config: DataSet, version: String) extends TaskTransform(name) {

  val term: Term = config("term").stringOption.getOrElse("").parse[Term].get
  val executor = new TermExecutor(config("namespace").stringOption.getOrElse("actio.datapipes.task.Term.Legacy.Functions"))

  def transform(dom: Dom): Seq[DataSet] = {
    if (version.contains("v2")) {
      if(config("behavior").stringOption.contains("batch"))
        List(executor.eval(dom.success, term))
      else if(config("behavior").stringOption.contains("expand"))
        List(DataArray(dom.success.flatMap(r => executor.eval(r, term).elems).toList))
      else
        List(DataArray(dom.success.map(r => executor.eval(r, term)).toList))
    }
    else
      executor.eval(dom.success, term).elems
  }

}
