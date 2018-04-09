package actio.datapipes.application

import actio.common.Data.{DataArray, DataNothing, DataSet}
import actio.common.{Dom, Event, EventAssertionFailed}
import actio.datapipes.application.AppConsole.logger
import actio.datapipes.pipeline.SimpleExecutor
import actio.datapipes.pipescript.Pipeline.PipeScript

object Executor {

  def run(pipeScript: PipeScript, pipeName: String, start: DataSet): Unit = {
    val startPipeline = pipeScript.pipelines.find(f => f.name == pipeName).get
    val pipeline = eventPipeline(pipeScript)(start)
    val runnable = SimpleExecutor.getRunnable(startPipeline.pipe, pipeline)

    logger.info(s"Running pipe: ${pipeScript.defaultPipeline}")

    // send start event
    pipeline.foreach { ep =>
      ep(List(Event.runStarted()))
    }

    // run the pipe
    runnable.start(start)

    // send the end event
    pipeline.foreach { ep =>
      ep(List(Event.runCompleted()))
    }

    pipeScript.pipelines.find(f => f.name == "p_datalake").toList.foreach{ p =>
      logger.info("Datalake pipeline detected. Running now...")
      val runnableUpload = SimpleExecutor.getRunnable(p.pipe, None)
      runnableUpload.start(start)
    }

    logger.info(s"Pipe ${pipeScript.defaultPipeline} completed successfully.")

    // TODO: return the statusCode
  }

  def run(pipeScript: PipeScript, start: DataSet): Unit = {
    run(pipeScript, pipeScript.defaultPipeline, start)
  }

  def eventPipeline(pipeScript: PipeScript)(start: DataSet) = {
    var statusCode = 0

    pipeScript.pipelines.find(f => f.name == "p_events")
      .map(e => {
        val run = SimpleExecutor.getRunnable(e.pipe, None)
        (events: List[Event]) => {

          val assertEvents = events.collect {
            case exit: EventAssertionFailed => exit
          }

          if (assertEvents.nonEmpty) {
            statusCode = assertEvents.last.statusCode
            assertEvents.foreach(ex => logger.warn(s"Validation failed: ${ex.message}"))
            if (assertEvents.exists(event => event.abort)) {
              logger.warn(s"Abort event detected, exiting with statuscode: $statusCode")
              Runtime.getRuntime.exit(statusCode)
            }
          }

          run
            .next(Dom() ~ Dom("start", Nil, start, DataNothing(), Nil) ~
              Dom("event", Nil, DataArray(events.map(Event.toDataSet)), DataNothing(), Nil))
        }
      })
  }
}
