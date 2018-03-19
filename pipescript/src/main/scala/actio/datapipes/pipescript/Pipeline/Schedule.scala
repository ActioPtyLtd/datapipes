package actio.datapipes.pipescript.Pipeline

import java.util.Date

trait Schedule

case class RunWithCronSchedule(startTime: Date, endTime: Date, cron: String) extends Schedule
case class RunWithInterval(startTime: Date, endTime: Date, seconds: Int) extends Schedule
case class RunOnce(expire: Date) extends Schedule
case class RunNever() extends Schedule