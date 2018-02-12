package actio.datapipes.application

import actio.common.Data.DataSet
import actio.datapipes.pipeline.SimpleExecutor
import actio.datapipes.pipescript.Pipeline.PipeScript

object Executor {

  def run(pipeScript: PipeScript, pipeName: String, start: DataSet): Unit = {
    val startPipeline = pipeScript.pipelines.find(f => f.name == pipeName).get
    val runnable = SimpleExecutor.getRunnable(startPipeline.pipe, None)

    runnable.start(start)
  }

  def run(pipeScript: PipeScript, start: DataSet): Unit = {
    run(pipeScript, pipeScript.defaultPipeline, start)
  }
}
