package actio.datapipes.application

import java.io.{File, FileFilter}
import java.time.{LocalDateTime, OffsetDateTime}

import actio.common.Data.DataSet
import actio.datapipes.pipeline.SimpleExecutor
import actio.datapipes.pipescript.Pipeline.{Builder, PipeScript}
import org.quartz._

object Scheduler {

  def getJobSchedule(pipeScript: PipeScript): List[(JobDetail, Trigger)] = {
    pipeScript.pipelines.flatMap(p => p.schedule.map(m => (m, p))).map(p => {
      val data = new JobDataMap()
      data.put("pipescript", pipeScript)
      data.put("pipename", p._2.name)

      (
        JobBuilder.newJob(classOf[PipeScriptJob]).setJobData(data).withIdentity(new JobKey(p._2.pipe.name)).build,
        TriggerBuilder.newTrigger.withIdentity(p._2.pipe.name).withSchedule(
          CronScheduleBuilder.cronSchedule(p._1.cron).withMisfireHandlingInstructionDoNothing
        ).build
      )
    })
  }

  def boot(pipeScript: PipeScript, start: DataSet): Unit = {
    import org.quartz.impl.StdSchedulerFactory
    val sf = new StdSchedulerFactory
    val sched = sf.getScheduler

    // get any schedules in system conf
    getJobSchedule(pipeScript).foreach { s =>
      sched.scheduleJob(s._1, s._2)
    }

    // get schedule to refresh config folder
    import org.apache.commons.io.monitor.FileAlterationObserver
    val observer = new FileAlterationObserver("./", new FileFilter {
      override def accept(file: File): Boolean = file.getName.endsWith(".conf")
    })
    observer.initialize()
    observer.addListener(new ConfigMonitorListener(sched))

    val data = new JobDataMap()
    data.put("observer", observer)

    sched.scheduleJob(
      JobBuilder.newJob(classOf[ConfigMonitorJob]).setJobData(data).withIdentity(new JobKey("observer")).build,
      TriggerBuilder.newTrigger.startNow().withSchedule(DailyTimeIntervalScheduleBuilder.dailyTimeIntervalSchedule().withIntervalInSeconds(5)).build
    )

    sched.start()

    System.in.read()

    sched.shutdown(true)
  }

  def boot(name: String, config: DataSet, start: DataSet): Unit = {
    val pipeScript = Builder.build(name, config)

    boot(pipeScript, start)
  }

}
