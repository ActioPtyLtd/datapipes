package actio.datapipes.application

import java.io.{File, FileFilter}
import java.text.SimpleDateFormat
import java.time.temporal.TemporalField
import java.time.{LocalDateTime, OffsetDateTime}
import java.util.Date

import actio.common.Data.DataSet
import actio.d.getCanonicalPathatapipes.application.ConfigMonitorListener
import actio.datapipes.pipeline.SimpleExecutor
import actio.datapipes.pipescript.Pipeline._
import com.typesafe.scalalogging.Logger
import org.apache.commons.io.FileUtils
import org.quartz._
import org.apache.commons.io.monitor.FileAlterationObserver
import org.quartz.impl.StdSchedulerFactory
import java.text.SimpleDateFormat

object Scheduler {
  lazy val logger = Logger("Scheduler")

  def getJobSchedule(pipeScript: PipeScript, file: File): List[(JobDetail, Trigger)] = {
    pipeScript.pipelines.map(p => (p.schedule,p)).filterNot(f => f._1.isInstanceOf[RunNever]).map(p => {
      val data = new JobDataMap()
      data.put("pipescript", file.getAbsolutePath)
      data.put("pipename", p._2.name)

      val now = new Date()

      val trigger = p._2.schedule match {
        case cron:RunWithCronSchedule => (t: TriggerBuilder[Trigger]) => {
          val ret = t.withSchedule(
            CronScheduleBuilder.cronSchedule(cron.cron).withMisfireHandlingInstructionDoNothing
          ).endAt(cron.endTime)
          if(cron.startTime.after(new Date()))
            ret.startAt(cron.startTime)
          else
            ret
        }
        case interval:RunWithInterval => (t: TriggerBuilder[Trigger]) => {
          val ret = t.withSchedule(
            SimpleScheduleBuilder.repeatSecondlyForever(interval.seconds).withMisfireHandlingInstructionIgnoreMisfires()
          ).endAt(interval.endTime)
          if(interval.startTime.after(new Date()))
            ret.startAt(interval.startTime)
          else
            ret
        }
        case once:RunOnce => (t: TriggerBuilder[Trigger]) =>
          t.startNow()
      }

      (
        JobBuilder.newJob(classOf[PipeScriptJob]).setJobData(data).withIdentity(new JobKey(p._2.pipe.name, pipeScript.name)).build,
        trigger(TriggerBuilder.newTrigger.withIdentity(p._2.pipe.name,pipeScript.name)).build
      )
    })
  }

  def getConfigMonitorJobSchedule(pipeScript: PipeScript): (JobDetail, Trigger, FileAlterationObserver) = {

    val directory = pipeScript.schedule.map(s => s.directory).getOrElse("./")

    val observer = new FileAlterationObserver(directory, new FileFilter {
      override def accept(file: File): Boolean = file.getName.endsWith(".conf")
    })
    observer.initialize()

    val data = new JobDataMap()
    data.put("observer", observer)

    (
      JobBuilder.newJob(classOf[ConfigMonitorJob]).setJobData(data).withIdentity(new JobKey("observer")).build,
      TriggerBuilder.newTrigger.startNow().withSchedule(DailyTimeIntervalScheduleBuilder.dailyTimeIntervalSchedule().withIntervalInSeconds(5)).build,
      observer
    )
  }

  def boot(pipeScript: PipeScript, file: File, start: DataSet): Unit = {
    val schedules = getJobSchedule(pipeScript, file)

    if (schedules.nonEmpty || pipeScript.schedule.isDefined) {
      val sched = new StdSchedulerFactory().getScheduler

      // get any schedules in current conf
      schedules.foreach { s =>
        sched.scheduleJob(s._1, s._2)
        logger.info(s"Job ${s._1.getKey.toString} added. Next run: ${s._2.getNextFireTime}.")
      }

      // get schedule to refresh config folder
      if (pipeScript.schedule.isDefined) {

        // find any confs to start with and create jobs if necessary
        import collection.JavaConverters._
        val confFiles = FileUtils.listFiles(new File(pipeScript.schedule.get.directory), Array("conf"), true).asScala.toList
        val builtConfigs = PipeScriptBuilder.build(confFiles)

        builtConfigs._1.foreach { p =>
          logger.info(s"Successfully parsed PipeScript: ${p._2.name}")
          val addSchedule = getJobSchedule(p._2, p._1)
          addSchedule.foreach { s =>
            sched.scheduleJob(s._1, s._2)
            logger.info(s"Job ${s._1.getKey.toString} added. Next run: ${s._2.getNextFireTime}.")
          }
          logger.info(s"${addSchedule.size} jobs found.")
        }
        builtConfigs._2.foreach { f =>
          logger.info(s"Skipping over file: ${f._1}...")
        }

        val configMonitorJobSchedule = getConfigMonitorJobSchedule(pipeScript)
        configMonitorJobSchedule._3.addListener(new ConfigMonitorListener(sched))

        sched.scheduleJob(configMonitorJobSchedule._1, configMonitorJobSchedule._2)
      }

      sched.start()

      Thread.currentThread.join()

      sched.shutdown(true)

    } else {
      Executor.run(pipeScript, start)
    }
  }

  def boot(fileName: String, start: DataSet): Unit = {
    val file = new File(fileName)
    val pipeScript = PipeScriptBuilder.build(file)
    boot(pipeScript.get, file, start)
  }


}
