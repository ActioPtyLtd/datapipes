package actio.datapipes.application

import java.io.File
import java.util.UUID

import actio.common.Data.{DataString, _}
import actio.datapipes.pipescript.Pipeline.PipeScript
import com.typesafe.scalalogging.Logger
import org.quartz.{DisallowConcurrentExecution, Job, JobExecutionContext, JobExecutionException}

@DisallowConcurrentExecution
class PipeScriptJob extends Job {
  lazy val logger = Logger("PipeScriptJob")

  override def execute(context: JobExecutionContext): Unit = {
    val data = context.getMergedJobDataMap

    val pipescript = data.get("pipescript").asInstanceOf[PipeScript]
    val executePipe = data.getString("pipename")
    val jobName = context.getJobDetail.getKey.toString

    logger.info(s"Triggering job: ${jobName}...")
    logger.info(s"Executing pipe: ${executePipe}")

    Executor.run(pipescript, executePipe, DataArray(startDataSet(executePipe, pipescript.name, UUID.randomUUID().toString)))
    
    logger.info(s"Execution completed for job: ${jobName}")
    logger.info(s"Next scheduled run: ${context.getNextFireTime}")
  }

  def startDataSet(pipeName: String, configName: String, scheduledRunId: String): DataSet = {
    import java.text.SimpleDateFormat
    val dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss")
    DataRecord(
      DataRecord(
        "run",
        DataString("id", UUID.randomUUID().toString),
        DataString("configName", configName),
        DataString("pipeName", pipeName),
        DataString("startDate", dateFormat.format(new java.util.Date())),
        DataString("scheduledRunId", scheduledRunId)
      )
    )
  }
}
