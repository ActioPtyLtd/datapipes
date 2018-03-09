package actio.datapipes.application

import java.io.File
import java.text.SimpleDateFormat
import java.util.UUID

import actio.common.Data.{DataString, _}
import actio.datapipes.pipescript.ConfigReader
import actio.datapipes.pipescript.Pipeline.{PipeScript, PipeScriptBuilder}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import org.quartz.{DisallowConcurrentExecution, Job, JobExecutionContext, JobExecutionException}

@DisallowConcurrentExecution
class PipeScriptJob extends Job {
  lazy val logger = Logger("PipeScriptJob")

  override def execute(context: JobExecutionContext): Unit = {
    val data = context.getMergedJobDataMap

    val file = data.get("pipescript").asInstanceOf[String]

    val executePipe = data.getString("pipename")
    val jobName = context.getJobDetail.getKey.toString
    val runid = UUID.randomUUID().toString
    val dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss")
    val rundate = dateFormat.format(new java.util.Date())

    val mergeConfig = initConfig(executePipe, runid, "", rundate, UUID.randomUUID().toString)
    val pipescript = PipeScriptBuilder.build(new File(file), mergeConfig).get

    logger.info(s"Triggering job: ${jobName}...")
    logger.info(s"Executing pipe: ${executePipe}")



    Executor.run(pipescript._1, executePipe, DataArray(startDataSet(executePipe, runid, pipescript._1.name, rundate, UUID.randomUUID().toString, pipescript._2)))
    
    logger.info(s"Execution completed for job: ${jobName}")
    logger.info(s"Next scheduled run: ${context.getNextFireTime}")
  }

  def initConfig(pipeName: String, runid: String, configName: String, rundate: String, scheduledRunId: String): String = {
s"""
run {
  id = "${runid}"
  configName = "${configName}"
  pipeName = "${pipeName}"
  startDate = "${rundate}"
  scheduledRunId = "${scheduledRunId}"
}"""
  }

  def startDataSet(pipeName: String, runid: String, configName: String, rundate: String, scheduledRunId: String, parameters: List[DataString]): DataSet = {
    import java.text.SimpleDateFormat

    DataRecord(
      List(
      DataRecord(
        "run",
        DataString("id", runid),
        DataString("configName", configName),
        DataString("pipeName", pipeName),
        DataString("startDate", rundate),
        DataString("scheduledRunId", scheduledRunId)
      )) ++
        parameters
    )
  }
}
