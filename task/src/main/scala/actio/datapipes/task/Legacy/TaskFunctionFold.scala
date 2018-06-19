package actio.datapipes.task.Legacy

import actio.common.Data.{DataNothing, DataSet, DataString}
import actio.common.Dom
import actio.datapipes.task.Term.FunctionExecutor
import actio.datapipes.task.TaskTransform
import org.apache.http.annotation.Obsolete

@Obsolete
class TaskFunctionFold(name: String, config: DataSet) extends TaskTransform(name) {

  val batch = config("batch").map(m => m.stringOption.map(_.split("\\s*,\\s*").toList).getOrElse(List()))

  // used to handle f1(dom, param1, param2, etc...)
  def transform(dom: Dom): Seq[DataSet] = List(
    batch
      .foldLeft[DataSet](dom.headOption.map(_.success).getOrElse(DataNothing()))((dataSet, funcList) =>
        funcList
          .headOption
          .map(h => FunctionExecutor.execute("", h, dataSet :: funcList.tail.map(DataString(_))))
          .getOrElse(DataNothing())))


}
