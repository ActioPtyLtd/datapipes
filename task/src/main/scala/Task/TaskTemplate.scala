package Task

import Common.Data.{DataNothing, DataRecord, DataString, Operators}
import Common.{DataSet, Dom, Parameters}

import scala.meta.Term
import scala.meta._
import _root_.Term.TermExecutor

class TaskTemplate(name: String, val config: DataSet) extends TaskTransform(name) {

  val templates = config("templates").elems.map(m => m.label -> m.stringOption.map(i => TermExecutor.interpolate(i)).getOrElse("").parse[Term])

  def transform(dom: Dom): Parameters =
    dom
      .headOption
      .map(h =>
        Operators.mergeLeft(
          h.success,
          DataRecord(templates.map(t => DataString(t._1, TermExecutor.eval(h.success,t._2.get).stringOption.getOrElse(""))).toList)
        )
      )
      .getOrElse(DataNothing())

}
