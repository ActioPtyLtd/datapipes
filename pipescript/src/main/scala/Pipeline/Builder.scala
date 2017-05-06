package Pipeline

import Common.DataSet

object Builder {

  def build(ds: DataSet, pipeName: String): Operation = {
    val pipeTasks =
      ds("script")("pipelines")(pipeName)("pipe")
        .stringOption
        .map(s => s.split("|").map(_.trim))

    val tasks = ds("script")("tasks")
  }
}
