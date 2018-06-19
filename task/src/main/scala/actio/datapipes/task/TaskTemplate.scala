package actio.datapipes.task

import actio.common.Data._
import actio.common.Dom

import scala.meta._
import actio.datapipes.task.Term.TermExecutor

class TaskTemplate(name: String, val config: DataSet, taskSetting: TaskSetting) extends TaskTransform(name) {

  val executor = new TermExecutor(taskSetting)

  val templates: Seq[(String, Parsed[scala.meta.Term])] = config("templates").map(m => m.label -> m.stringOption.map(i => executor.interpolate(i)).getOrElse("").parse[Term])

  def transform(dom: Dom): Seq[DataSet] =
    List(DataArray(dom.success.map(s => {
        if (taskSetting.version.contains("v2"))
          Operators.mergeLeft(
            s,
            DataRecord(templates.map(t => DataString(t._1, executor.eval(s, t._2.get).stringOption.getOrElse(""))).toList)
          )
        else
          DataRecord(s :: templates.map(t => DataString(t._1, executor.eval(s, t._2.get).stringOption.getOrElse(""))).toList)
      }).toList))
}
