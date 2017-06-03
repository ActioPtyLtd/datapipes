package Task

import DataPipes.Common._
import DataPipes.Common.Data._

import scala.meta.Term
import scala.meta._
import _root_.Term.TermExecutor

class TaskTemplate(name: String, val config: DataSet, version: String) extends TaskTransform(name) {

  val executor = new TermExecutor(config("namespace").stringOption.getOrElse("Term.Legacy.Functions"))

  val templates: Seq[(String, Parsed[Term])] = config("templates").map(m => m.label -> m.stringOption.map(i => executor.interpolate(i)).getOrElse("").parse[Term])

  def transform(dom: Dom): Seq[DataSet] =
    dom
      .headOption
      .toList
      .map(h => DataArray(h.success.map(s => {
        if (version.contains("v2"))
          Operators.mergeLeft(
            s,
            DataRecord(templates.map(t => DataString(t._1, executor.eval(s, t._2.get).stringOption.getOrElse(""))).toList)
          )
        else
          DataRecord(s :: templates.map(t => DataString(t._1, executor.eval(s, t._2.get).stringOption.getOrElse(""))).toList)
      }).toList))
}
