package actio.datapipes.task.Legacy

import DataPipes.Common.Data._
import DataPipes.Common.Dom
import actio.datapipes.task.Term.{FunctionExecutor, TermExecutor}
import actio.datapipes.task.TaskTransform


class TaskFunctionFold(name: String, config: DataSet) extends TaskTransform(name) {

  val namespace = config("namespace").stringOption.getOrElse("actio.datapipes.task.Term.Functions")

  val batch = config("batch").map(m => m.stringOption.map(_.split("\\s*,\\s*").toList).getOrElse(List()))

  // used to handle f1(dom, param1, param2, etc...)
  def transform(dom: Dom): Seq[DataSet] = List(
    batch
      .foldLeft[DataSet](dom.headOption.map(_.success).getOrElse(DataNothing()))((dataSet, funcList) =>
        funcList
          .headOption
          .map(h => FunctionExecutor.execute(namespace, h, dataSet :: funcList.tail.map(DataString(_))))
          .getOrElse(DataNothing())))


}
