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
    val data = context.getMergedJobDataMap

    val pipescript = data.get("pipescript").asInstanceOf[PipeScript]
    val executePipe = data.getString("pipename")
    val jobName = context.getJobDetail.getKey.getName

    logger.info(s"Triggering job: ${jobName}...")
    logger.info(s"Executing pipe: ${executePipe}")

    Executor.run(pipescript, executePipe, DataArray(DataNothing()))

    logger.info(s"Execution completed for job: ${jobName}")
    logger.info(s"Next scheduled run: ${context.getNextFireTime}")
  }
}
