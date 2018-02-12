package actio.datapipes.application

import com.typesafe.scalalogging.Logger
import org.apache.commons.io.monitor.FileAlterationObserver
import org.quartz.{DisallowConcurrentExecution, Job, JobExecutionContext}

@DisallowConcurrentExecution
class ConfigMonitorJob extends Job {
  lazy val logger = Logger("ConfigMonitorJob")

  override def execute(context: JobExecutionContext): Unit = {

    val data = context.getMergedJobDataMap
    val observer = data.get("observer").asInstanceOf[FileAlterationObserver]

    observer.checkAndNotify()
  }
}
