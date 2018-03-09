package actio.d.getCanonicalPathatapipes.application

import java.io.File

import actio.datapipes.pipescript.ConfigReader
import actio.datapipes.pipescript.Pipeline.PipeScriptBuilder
import com.typesafe.scalalogging.Logger
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor
import org.quartz.Scheduler
import org.quartz.impl.matchers.GroupMatcher

class ConfigMonitorListener(scheduler: Scheduler) extends FileAlterationListenerAdaptor {
  lazy val logger = Logger("ConfigMonitorListener")

  override def onFileChange(file: File) : Unit = {
    logger.info(s"File change detected: ${file.toString}")
    processFile(file)
  }

  override def onFileCreate(file: File): Unit = {
    logger.info(s"New file detected: ${file.toString}")
    processFile(file)
  }

  override def onFileDelete(file: File): Unit = {
    //scheduler.getJobKeys(GroupMatcher.groupEquals(file.toString))
  }



  def processFile(file: File): Unit = {
    val config = ConfigReader.read(file)
    val pipeScript = PipeScriptBuilder.build(file.toString, config)

    val jobList = actio.datapipes.application.Scheduler.getJobSchedule(pipeScript, file)

    import collection.JavaConverters._
    val currentJobsForFile = scheduler.getJobKeys(GroupMatcher.groupEquals(file.toString)).asScala
    currentJobsForFile.foreach { k =>
      logger.info(s"Removing job: ${k.toString}")
      scheduler.deleteJob(k)
    }

    jobList.foreach(j => {
      if(scheduler.checkExists(j._1.getKey)) {
        logger.info(s"Updating existing job: ${j._1.getKey.toString}...")
        scheduler.deleteJob(j._1.getKey)
      } else {
        logger.info(s"Creating new job: ${j._1.getKey.toString}...")
      }

      scheduler.scheduleJob(j._1, j._2)
      logger.info(s"Next scheduled run: ${j._2.getNextFireTime}")
    })
  }
}
