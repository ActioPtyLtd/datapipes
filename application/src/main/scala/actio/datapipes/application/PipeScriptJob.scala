package actio.datapipes.application

import actio.common.Data.{DataArray, DataNothing}
import actio.datapipes.pipescript.Pipeline.PipeScript
import com.typesafe.scalalogging.Logger
import org.quartz.Job
import org.quartz.JobExecutionContext
import org.quartz.JobExecutionException


class PipeScriptJob extends Job {
  lazy val logger = Logger("PipeScriptJob")

  override def execute(context: JobExecutionContext): Unit = {
    logger.info("Job triggered...")

    val data = context.getMergedJobDataMap()

    val pipescript = data.get("pipescript").asInstanceOf[PipeScript]
    val executePipe = data.getString("pipename")

    logger.info(s"Executing pipe: ${executePipe}")

    Executor.run(pipescript, executePipe, DataArray(DataNothing()))
  }
}
