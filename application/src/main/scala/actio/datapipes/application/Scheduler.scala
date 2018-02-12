package actio.datapipes.application

import java.io.{File, FileFilter}
import java.time.{LocalDateTime, OffsetDateTime}

import actio.common.Data.DataSet
import actio.d.getCanonicalPathatapipes.application.ConfigMonitorListener
import actio.datapipes.pipeline.SimpleExecutor
import actio.datapipes.pipescript.Pipeline.{Builder, PipeScript}
import org.quartz._
import org.apache.commons.io.monitor.FileAlterationObserver
import org.quartz.impl.StdSchedulerFactory

object Scheduler {

  def getJobSchedule(pipeScript: PipeScript): List[(JobDetail, Trigger)] = {
    pipeScript.pipelines.flatMap(p => p.schedule.map(m => (m, p))).map(p => {
      val data = new JobDataMap()
      data.put("pipescript", pipeScript)
      data.put("pipename", p._2.name)

      (
        JobBuilder.newJob(classOf[PipeScriptJob]).setJobData(data).withIdentity(new JobKey(pipeScript.name + "(" + p._2.pipe.name + ")")).build,
        TriggerBuilder.newTrigger.withIdentity(p._2.pipe.name).withSchedule(
          CronScheduleBuilder.cronSchedule(p._1.cron).withMisfireHandlingInstructionDoNothing
        ).build
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

  def boot(pipeScript: PipeScript, start: DataSet): Unit = {
    val schedules = getJobSchedule(pipeScript)

    if (schedules.nonEmpty || pipeScript.schedule.isDefined) {
      val sched = new StdSchedulerFactory().getScheduler

      // get any schedules in system conf
      schedules.foreach { s =>
        sched.scheduleJob(s._1, s._2)
      }

      // get schedule to refresh config folder
      if (pipeScript.schedule.isDefined) {
        val configMonitorJobSchedule = getConfigMonitorJobSchedule(pipeScript)
        configMonitorJobSchedule._3.addListener(new ConfigMonitorListener(sched))

        sched.scheduleJob(configMonitorJobSchedule._1, configMonitorJobSchedule._2)
      }

      if (schedules.nonEmpty) {
        sched.start()

        System.in.read()

        sched.shutdown(true)
      }
    } else {
      Executor.run(pipeScript, start)
    }
  }

  def boot(name: String, config: DataSet, start: DataSet): Unit = {
    val pipeScript = Builder.build(name, config)

    boot(pipeScript, start)
  }

}
