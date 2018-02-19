package actio.datapipes.pipescript.Pipeline

import java.util.Date

case class Schedule(startTime: Option[Date], cron: String)